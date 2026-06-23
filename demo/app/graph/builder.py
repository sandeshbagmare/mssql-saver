"""Compile the text-analysis graph with a given checkpointer."""
from langgraph.graph import END, StateGraph

from .nodes import analyze, normalize, summarize
from .state import TextAnalysisState


def build_graph(checkpointer=None):
    builder = StateGraph(TextAnalysisState)

    builder.add_node("normalize", normalize)
    builder.add_node("analyze", analyze)
    builder.add_node("summarize", summarize)

    builder.set_entry_point("normalize")
    builder.add_edge("normalize", "analyze")
    builder.add_edge("analyze", "summarize")
    builder.add_edge("summarize", END)

    return builder.compile(checkpointer=checkpointer)
