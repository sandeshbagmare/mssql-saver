"""LangGraph state definition for the deterministic text-analysis graph."""
from typing import TypedDict


class TextAnalysisState(TypedDict):
    text: str           # raw input text (set by user)
    normalised: str     # lowercased + stripped
    word_count: int
    char_count: int
    sentence_count: int
    summary: str        # final output string
