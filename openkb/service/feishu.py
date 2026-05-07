from __future__ import annotations

import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests


class FeishuExportError(RuntimeError):
    """Raised when a Feishu document cannot be exported to PDF."""


@dataclass(frozen=True)
class FeishuDoc:
    url: str
    doc_type: str
    token: str


_DOC_RE = re.compile(r"/(docx|docs|doc|wiki)/([A-Za-z0-9]+)")


def is_feishu_url(value: str) -> bool:
    host = urlparse(value).netloc.lower()
    return "feishu.cn" in host or "larksuite.com" in host


def export_feishu_pdf(url: str, output_dir: Path) -> Path:
    """Export a Feishu document URL to a local PDF.

    The preferred built-in path uses Feishu OpenAPI credentials:
    FEISHU_APP_ID + FEISHU_APP_SECRET, or FEISHU_TENANT_ACCESS_TOKEN.

    For private enterprise setups, FEISHU_EXPORT_COMMAND can point to an
    existing exporter command. It may include {url} and {output} placeholders.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    doc = parse_feishu_doc(url)
    output = output_dir / f"feishu_{doc.token}.pdf"

    command = os.environ.get("FEISHU_EXPORT_COMMAND", "").strip()
    if command:
        _run_export_command(command, url, output)
        return output

    token = _tenant_access_token()
    ticket = _create_export_task(doc, token)
    file_token = _wait_export_task(doc, token, ticket)
    _download_export_file(token, file_token, output)
    return output


def parse_feishu_doc(url: str) -> FeishuDoc:
    match = _DOC_RE.search(url)
    if not match:
        raise FeishuExportError(f"无法从飞书链接解析文档 token: {url}")

    raw_type, token = match.groups()
    doc_type = "docx" if raw_type in {"docx", "wiki"} else "doc"
    return FeishuDoc(url=url, doc_type=doc_type, token=token)


def _run_export_command(command: str, url: str, output: Path) -> None:
    rendered = command.format(url=url, output=str(output))
    args = shlex.split(rendered, posix=False)
    completed = subprocess.run(args, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise FeishuExportError(
            "FEISHU_EXPORT_COMMAND failed: "
            f"{completed.stderr or completed.stdout or completed.returncode}"
        )
    if not output.exists():
        raise FeishuExportError(f"FEISHU_EXPORT_COMMAND did not create PDF: {output}")


def _openapi_base() -> str:
    return os.environ.get("FEISHU_OPENAPI_BASE", "https://open.feishu.cn/open-apis").rstrip("/")


def _tenant_access_token() -> str:
    direct = os.environ.get("FEISHU_TENANT_ACCESS_TOKEN", "").strip()
    if direct:
        return direct

    app_id = os.environ.get("FEISHU_APP_ID", "").strip()
    app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        raise FeishuExportError(
            "飞书链接导出 PDF 需要配置 FEISHU_APP_ID/FEISHU_APP_SECRET，"
            "或 FEISHU_TENANT_ACCESS_TOKEN，或 FEISHU_EXPORT_COMMAND。"
        )

    resp = requests.post(
        f"{_openapi_base()}/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=30,
    )
    data = _json(resp)
    token = data.get("tenant_access_token") or data.get("data", {}).get("tenant_access_token")
    if not token:
        raise FeishuExportError(f"获取 tenant_access_token 失败: {data}")
    return str(token)


def _create_export_task(doc: FeishuDoc, token: str) -> str:
    resp = requests.post(
        f"{_openapi_base()}/drive/v1/export_tasks",
        headers={"Authorization": f"Bearer {token}"},
        json={"file_extension": "pdf", "token": doc.token, "type": doc.doc_type},
        timeout=30,
    )
    data = _json(resp)
    ticket = data.get("data", {}).get("ticket") or data.get("ticket")
    if not ticket:
        raise FeishuExportError(f"创建飞书导出任务失败: {data}")
    return str(ticket)


def _wait_export_task(doc: FeishuDoc, token: str, ticket: str) -> str:
    for _ in range(60):
        resp = requests.get(
            f"{_openapi_base()}/drive/v1/export_tasks/{ticket}",
            headers={"Authorization": f"Bearer {token}"},
            params={"token": doc.token},
            timeout=30,
        )
        data = _json(resp)
        result = data.get("data", {}).get("result") or data.get("result") or {}
        file_token = result.get("file_token") or data.get("data", {}).get("file_token")
        if file_token:
            return str(file_token)
        status = result.get("job_status") or data.get("data", {}).get("job_status")
        if status in {"failed", "Fail", "FAILED"}:
            raise FeishuExportError(f"飞书导出失败: {data}")
        time.sleep(1)
    raise FeishuExportError("飞书导出任务超时")


def _download_export_file(token: str, file_token: str, output: Path) -> None:
    resp = requests.get(
        f"{_openapi_base()}/drive/v1/export_tasks/file/{file_token}/download",
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    if resp.status_code >= 400:
        raise FeishuExportError(f"下载飞书导出 PDF 失败: HTTP {resp.status_code} {resp.text[:200]}")
    output.write_bytes(resp.content)


def _json(resp: requests.Response) -> dict:
    try:
        data = resp.json()
    except ValueError as exc:
        raise FeishuExportError(f"飞书接口返回非 JSON: HTTP {resp.status_code}") from exc
    if resp.status_code >= 400 or data.get("code", 0) not in (0, "0"):
        raise FeishuExportError(f"飞书接口错误: HTTP {resp.status_code} {data}")
    return data
