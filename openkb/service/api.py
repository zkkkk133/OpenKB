from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from openkb.service.manager import SERVICE


app = FastAPI(title="OpenKB Service", version="0.1.0")


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})


@app.exception_handler(FileNotFoundError)
async def file_not_found_handler(request: Request, exc: FileNotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"ok": False, "error": str(exc)})


class AskRequest(BaseModel):
    question: str
    projectPath: str | None = None
    username: str | None = None
    useCache: bool | None = None
    cacheTtlDays: int | None = 5
    tags: list[str] = Field(default_factory=list)


class KnowledgeTextRequest(BaseModel):
    title: str
    content: str
    projectPath: str | None = None
    username: str | None = None
    useCache: bool | None = None
    tags: list[str] = Field(default_factory=list)


class UploadedFilePayload(BaseModel):
    relativePath: str | None = None
    filename: str
    mimeType: str | None = None
    contentBase64: str


class KnowledgeFilesRequest(BaseModel):
    title: str | None = None
    sourceType: str = "file"
    projectPath: str | None = None
    username: str | None = None
    useCache: bool | None = None
    tags: list[str] = Field(default_factory=list)
    files: list[UploadedFilePayload]


class KnowledgeSourceRequest(BaseModel):
    source: str
    sourceType: str | None = None
    projectPath: str | None = None
    username: str | None = None
    recursive: bool = True
    useCache: bool | None = None
    tags: list[str] = Field(default_factory=list)


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True}


@app.post("/ask/enqueue")
def ask_enqueue(req: AskRequest) -> dict[str, Any]:
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question is required")
    job = SERVICE.submit_query(req.model_dump())
    return {"ok": True, **(SERVICE.ask_status(job.id) or job.public())}


@app.get("/ask/status")
def ask_status(id: str = Query(...)) -> dict[str, Any]:
    job = SERVICE.ask_status(id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"ask job not found: {id}")
    return {"ok": True, **job}


@app.post("/knowledge/text")
def knowledge_text(req: KnowledgeTextRequest) -> dict[str, Any]:
    if not req.content:
        raise HTTPException(status_code=400, detail="content is required")
    job = SERVICE.submit_text(req.model_dump())
    return {"ok": True, **(SERVICE.knowledge_status(job.id) or job.public())}


@app.post("/knowledge/files")
def knowledge_files(req: KnowledgeFilesRequest) -> dict[str, Any]:
    if not req.files:
        raise HTTPException(status_code=400, detail="files is required")
    job = SERVICE.submit_files(req.model_dump())
    return {"ok": True, **(SERVICE.knowledge_status(job.id) or job.public())}


@app.post("/knowledge/source")
def knowledge_source(req: KnowledgeSourceRequest) -> dict[str, Any]:
    if not req.source.strip():
        raise HTTPException(status_code=400, detail="source is required")
    job = SERVICE.submit_source(req.model_dump())
    return {"ok": True, **(SERVICE.knowledge_status(job.id) or job.public())}


@app.get("/knowledge/status")
def knowledge_status(id: str = Query(...)) -> dict[str, Any]:
    job = SERVICE.knowledge_status(id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"knowledge job not found: {id}")
    return {"ok": True, **job}


@app.get("/queues")
def queues() -> dict[str, Any]:
    return {"ok": True, **SERVICE.queues()}


def main() -> None:
    import uvicorn

    host = os.environ.get("OPENKB_API_HOST", "127.0.0.1")
    port = int(os.environ.get("OPENKB_API_PORT", "19827"))
    uvicorn.run("openkb.service.api:app", host=host, port=port)


if __name__ == "__main__":
    main()
