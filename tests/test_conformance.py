"""Conformance tests for MssqlSaver.

Run against a live SQL Server instance:

    export MSSQL_TEST_CONN_STR="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=langgraph_test;UID=sa;PWD=SqlPass123!;Encrypt=yes;TrustServerCertificate=yes;"
    pytest tests/test_conformance.py -v
"""
from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

from langgraph.checkpoint.base import empty_checkpoint
from langgraph_checkpoint_mssql import MssqlSaver

CONN_STR = os.environ.get(
    "MSSQL_TEST_CONN_STR",
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;DATABASE=langgraph_test;"
    "UID=sa;PWD=SqlPass123!;"
    "Encrypt=yes;TrustServerCertificate=yes;",
)


@pytest.fixture(scope="module")
def saver():
    with MssqlSaver(CONN_STR, pool_size=25) as s:
        s.setup()
        yield s


def _config(thread_id: str, checkpoint_id: str | None = None, ns: str = "") -> dict:
    c: dict = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ns}}
    if checkpoint_id:
        c["configurable"]["checkpoint_id"] = checkpoint_id
    return c


def _checkpoint(idx: int = 0) -> dict:
    c = empty_checkpoint()
    c["channel_values"] = {"counter": idx}
    c["channel_versions"] = {"counter": f"{idx + 1:032}.0"}
    return c


# ---------------------------------------------------------------------------
# Basic put / get_tuple round-trip
# ---------------------------------------------------------------------------

def test_put_get_tuple_latest(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    config = _config(tid)
    ckpt = _checkpoint(0)
    new_config = saver.put(config, ckpt, {"source": "input", "step": 0}, ckpt["channel_versions"])

    assert new_config["configurable"]["checkpoint_id"] == ckpt["id"]

    result = saver.get_tuple(_config(tid))
    assert result is not None
    assert result.checkpoint["id"] == ckpt["id"]
    assert result.checkpoint["channel_values"]["counter"] == 0
    assert result.parent_config is None


def test_put_get_tuple_by_id(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    ckpt1 = _checkpoint(1)
    ckpt2 = _checkpoint(2)
    cfg1 = saver.put(_config(tid), ckpt1, {"step": 1}, ckpt1["channel_versions"])
    saver.put(cfg1, ckpt2, {"step": 2}, ckpt2["channel_versions"])

    # Fetch the first checkpoint explicitly by ID
    result = saver.get_tuple(_config(tid, ckpt1["id"]))
    assert result is not None
    assert result.checkpoint["id"] == ckpt1["id"]
    assert result.checkpoint["channel_values"]["counter"] == 1


def test_latest_is_most_recent(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    for i in range(3):
        ckpt = _checkpoint(i)
        saver.put(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])

    result = saver.get_tuple(_config(tid))
    assert result is not None
    assert result.checkpoint["channel_values"]["counter"] == 2


# ---------------------------------------------------------------------------
# Parent config tracking
# ---------------------------------------------------------------------------

def test_parent_config(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    ckpt1 = _checkpoint(0)
    cfg1 = saver.put(_config(tid), ckpt1, {"step": 0}, ckpt1["channel_versions"])
    ckpt2 = _checkpoint(1)
    saver.put(cfg1, ckpt2, {"step": 1}, ckpt2["channel_versions"])

    result = saver.get_tuple(_config(tid))
    assert result is not None
    assert result.parent_config is not None
    assert result.parent_config["configurable"]["checkpoint_id"] == ckpt1["id"]


# ---------------------------------------------------------------------------
# list()
# ---------------------------------------------------------------------------

def test_list_returns_descending(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    ids = []
    for i in range(4):
        ckpt = _checkpoint(i)
        c = saver.put(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])
        ids.append(c["configurable"]["checkpoint_id"])

    results = list(saver.list(_config(tid)))
    result_ids = [r.checkpoint["id"] for r in results]
    assert result_ids == sorted(result_ids, reverse=True)


def test_list_limit(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    for i in range(5):
        ckpt = _checkpoint(i)
        saver.put(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])

    results = list(saver.list(_config(tid), limit=3))
    assert len(results) == 3


def test_list_before(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    configs = []
    for i in range(4):
        ckpt = _checkpoint(i)
        c = saver.put(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])
        configs.append(c)

    # before = configs[2] → should return configs[0] and configs[1]
    results = list(saver.list(_config(tid), before=configs[2]))
    result_ids = {r.checkpoint["id"] for r in results}
    assert configs[2]["configurable"]["checkpoint_id"] not in result_ids
    assert configs[3]["configurable"]["checkpoint_id"] not in result_ids


def test_list_filter_metadata(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    for i in range(4):
        ckpt = _checkpoint(i)
        src = "input" if i % 2 == 0 else "loop"
        saver.put(_config(tid), ckpt, {"source": src, "step": i}, ckpt["channel_versions"])

    results = list(saver.list(_config(tid), filter={"source": "loop"}))
    assert len(results) == 2
    for r in results:
        assert r.metadata.get("source") == "loop"


# ---------------------------------------------------------------------------
# put_writes / pending writes round-trip
# ---------------------------------------------------------------------------

def test_put_writes_and_retrieve(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    ckpt = _checkpoint(0)
    cfg = saver.put(_config(tid), ckpt, {"step": 0}, ckpt["channel_versions"])

    task_id = str(uuid.uuid4())
    saver.put_writes(cfg, [("output", {"result": 42}), ("status", "ok")], task_id)

    result = saver.get_tuple(cfg)
    assert result is not None
    channels = {w[1] for w in result.pending_writes}
    assert "output" in channels
    assert "status" in channels


def test_put_writes_dedup_regular(saver: MssqlSaver):
    """A second put_writes with the same task_id+channel overwrites (idx>=0)."""
    tid = f"test-{uuid.uuid4()}"
    ckpt = _checkpoint(0)
    cfg = saver.put(_config(tid), ckpt, {"step": 0}, ckpt["channel_versions"])
    task_id = str(uuid.uuid4())

    saver.put_writes(cfg, [("out", "v1")], task_id)
    saver.put_writes(cfg, [("out", "v2")], task_id)

    result = saver.get_tuple(cfg)
    assert result is not None
    out_writes = [w for w in result.pending_writes if w[1] == "out"]
    assert len(out_writes) == 1
    assert out_writes[0][2] == "v2"


# ---------------------------------------------------------------------------
# delete_thread
# ---------------------------------------------------------------------------

def test_delete_thread(saver: MssqlSaver):
    tid = f"test-{uuid.uuid4()}"
    for i in range(3):
        ckpt = _checkpoint(i)
        c = saver.put(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])
        saver.put_writes(c, [("x", i)], str(uuid.uuid4()))

    saver.delete_thread(tid)

    assert saver.get_tuple(_config(tid)) is None
    assert list(saver.list(_config(tid))) == []


# ---------------------------------------------------------------------------
# get_next_version monotonicity
# ---------------------------------------------------------------------------

def test_version_monotonic(saver: MssqlSaver):
    versions = [saver.get_next_version(None)]
    for _ in range(9):
        versions.append(saver.get_next_version(versions[-1]))
    assert versions == sorted(versions)


# ---------------------------------------------------------------------------
# Concurrency: multiple threads writing different thread_ids simultaneously
# ---------------------------------------------------------------------------

def test_concurrent_writes(saver: MssqlSaver):
    errors: list[Exception] = []

    def worker(n: int):
        try:
            tid = f"concurrent-{n}-{uuid.uuid4()}"
            for i in range(5):
                ckpt = _checkpoint(i)
                cfg = saver.put(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])
                saver.put_writes(cfg, [("v", i)], str(uuid.uuid4()))
            result = saver.get_tuple(_config(tid))
            assert result is not None
            assert result.checkpoint["channel_values"]["counter"] == 4
            saver.delete_thread(tid)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Concurrent errors: {errors}"


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_put_get(saver: MssqlSaver):
    tid = f"async-{uuid.uuid4()}"
    ckpt = _checkpoint(7)
    cfg = await saver.aput(_config(tid), ckpt, {"step": 7}, ckpt["channel_versions"])

    result = await saver.aget_tuple(cfg)
    assert result is not None
    assert result.checkpoint["channel_values"]["counter"] == 7
    await saver.adelete_thread(tid)


@pytest.mark.asyncio
async def test_async_list(saver: MssqlSaver):
    tid = f"async-list-{uuid.uuid4()}"
    for i in range(3):
        ckpt = _checkpoint(i)
        await saver.aput(_config(tid), ckpt, {"step": i}, ckpt["channel_versions"])

    results = [r async for r in saver.alist(_config(tid))]
    assert len(results) == 3
    await saver.adelete_thread(tid)
