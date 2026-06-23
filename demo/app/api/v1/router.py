from fastapi import APIRouter
from app.api.v1.endpoints.graph import router as graph_router

api_router = APIRouter()
api_router.include_router(graph_router, prefix="/graph", tags=["graph"])
