"""Graph API endpoints — invoke and history for both backends."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.graph import CheckpointInfo, HistoryResponse, InvokeRequest, InvokeResponse
from app.services.graph_service import get_thread_history, invoke_graph

router = APIRouter()

VALID_BACKENDS = {"postgres", "mssql"}


def _check_backend(backend: str) -> str:
    if backend not in VALID_BACKENDS:
        raise HTTPException(status_code=400, detail=f"backend must be one of {VALID_BACKENDS}")
    return backend


@router.post("/{backend}/invoke", response_model=InvokeResponse, summary="Run the graph")
async def invoke(
    backend: str,
    body: InvokeRequest,
    db: Session = Depends(get_db),
):
    """Invoke the text-analysis graph using the specified checkpoint backend.

    - **backend**: `postgres` or `mssql`
    - **text**: input text to process
    - **thread_id**: optional; reuse to accumulate state across calls
    """
    _check_backend(backend)
    result = await invoke_graph(backend, body.text, body.thread_id, db)
    return result


@router.get("/{backend}/history/{thread_id}", response_model=HistoryResponse,
            summary="List checkpoints for a thread")
async def history(backend: str, thread_id: str):
    """Return the checkpoint history for *thread_id* from the given backend."""
    _check_backend(backend)
    checkpoints = get_thread_history(backend, thread_id)
    return HistoryResponse(
        thread_id=thread_id,
        backend=backend,
        checkpoints=[CheckpointInfo(**c) for c in checkpoints],
    )
