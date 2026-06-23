"""Deterministic graph nodes — no LLM, no network calls.

Three nodes:
  normalize  →  analyze  →  summarize

This keeps the graph fast and reproducible, isolating checkpointer latency
from any external variability in benchmark runs.
"""
from .state import TextAnalysisState


def normalize(state: TextAnalysisState) -> dict:
    return {"normalised": state["text"].lower().strip()}


def analyze(state: TextAnalysisState) -> dict:
    text = state["normalised"]
    words = text.split()
    sentences = [s.strip() for s in text.replace("!", ".").replace("?", ".").split(".") if s.strip()]
    return {
        "word_count": len(words),
        "char_count": len(text),
        "sentence_count": len(sentences),
    }


def summarize(state: TextAnalysisState) -> dict:
    summary = (
        f"Text of {state['char_count']} chars, "
        f"{state['word_count']} words, "
        f"{state['sentence_count']} sentence(s). "
        f'Preview: "{state["normalised"][:60]}{"..." if len(state["normalised"]) > 60 else ""}"'
    )
    return {"summary": summary}
