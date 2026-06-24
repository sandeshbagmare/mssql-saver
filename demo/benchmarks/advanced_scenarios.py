"""Advanced benchmark scenarios for LangGraph MSSQL Checkpoint Saver."""
import asyncio
import json
import random
import string
import time
import uuid

from langgraph.checkpoint.base import CheckpointTuple
from langgraph.graph import END, StateGraph
from app.services.checkpointer_factory import get_checkpointer


def generate_large_text(size_kb: int) -> str:
    """Generate a random string of approximately size_kb KB."""
    return "".join(random.choices(string.ascii_letters + " ", k=size_kb * 1024))


def run_high_payload_benchmark():
    print("\n--- Scenario 11: High Payload Simulation ---")
    
    # 50 KB payload
    large_text = generate_large_text(50)
    
    for backend in ["mssql"]:
        checkpointer = get_checkpointer(backend)
        from app.graph.builder import build_graph
        graph = build_graph(checkpointer)
        
        thread_id = str(uuid.uuid4())
        config = {"configurable": {"thread_id": thread_id}}
        
        start = time.perf_counter()
        # Warmup
        graph.invoke({"text": "short"}, config)
        
        # 10 invocations with 50KB payload
        latencies = []
        for _ in range(10):
            t0 = time.perf_counter()
            graph.invoke({"text": large_text}, config)
            latencies.append((time.perf_counter() - t0) * 1000)
            
        print(f"[{backend.upper()}] 50KB Payload (10 invs): p50 = {sorted(latencies)[5]:.2f} ms")


def run_long_history_benchmark():
    print("\n--- Scenario 12: Long Conversation History Simulation ---")
    
    for backend in ["mssql"]:
        checkpointer = get_checkpointer(backend)
        from app.graph.builder import build_graph
        graph = build_graph(checkpointer)
        
        thread_id = str(uuid.uuid4())
        config = {"configurable": {"thread_id": thread_id}}
        
        latencies = []
        # 100 turns in one thread
        for i in range(100):
            t0 = time.perf_counter()
            graph.invoke({"text": f"Turn {i} text analysis input."}, config)
            latencies.append((time.perf_counter() - t0) * 1000)
            
        print(f"[{backend.upper()}] Latency evolution over 100 turns:")
        print(f"  Turn 1:   {latencies[0]:.2f} ms")
        print(f"  Turn 10:  {latencies[9]:.2f} ms")
        print(f"  Turn 50:  {latencies[49]:.2f} ms")
        print(f"  Turn 100: {latencies[99]:.2f} ms")


def run_time_travel_benchmark():
    print("\n--- Scenario 13: Time-Travel & History Forking ---")
    
    for backend in ["mssql"]:
        checkpointer = get_checkpointer(backend)
        from app.graph.builder import build_graph
        graph = build_graph(checkpointer)
        
        thread_id = str(uuid.uuid4())
        config = {"configurable": {"thread_id": thread_id}}
        
        # 20 turns
        for i in range(20):
            graph.invoke({"text": f"Base history {i}"}, config)
            
        # Get history
        history = list(checkpointer.list(config))
        # Find checkpoint from roughly half-way (turn 10 out of 20 -> history is reverse ordered)
        # 20 invocations * 3 nodes/inv = 60 checkpoints total. 
        # We'll just grab the 30th from the list (which is around turn 10)
        target_checkpoint = history[30]
        fork_config = target_checkpoint.config
        
        # Now time travel: invoke from this past config
        t0 = time.perf_counter()
        graph.invoke({"text": "Forked reality"}, fork_config)
        fork_latency = (time.perf_counter() - t0) * 1000
        
        # Verify it worked by checking the new latest checkpoint parent chain
        new_latest = checkpointer.get_tuple(config)
        
        print(f"[{backend.upper()}] Time-travel fork latency: {fork_latency:.2f} ms")
        print(f"[{backend.upper()}] Forked successfully from checkpoint {fork_config['configurable']['checkpoint_id']} -> new latest is {new_latest.config['configurable']['checkpoint_id']}")


def run_interrupt_benchmark():
    print("\n--- Scenario 14: Interrupt / Human-in-the-Loop ---")
    
    # We will just benchmark `put_writes` with an INTERRUPT channel
    for backend in ["mssql"]:
        checkpointer = get_checkpointer(backend)
        
        thread_id = str(uuid.uuid4())
        config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
        from app.graph.builder import build_graph
        graph = build_graph(checkpointer)
        # Run graph once to create a valid checkpoint
        graph.invoke({"text": "Hello"}, config)
        
        # Get the config of the latest checkpoint
        config = checkpointer.get_tuple(config).config
        
        t0 = time.perf_counter()
        checkpointer.put_writes(
            config,
            writes=[("__interrupt__", "Paused for human review")],
            task_id="task_123"
        )
        interrupt_latency = (time.perf_counter() - t0) * 1000
        
        # Verify
        tup = checkpointer.get_tuple(config)
        has_interrupt = any(w[0] == "task_123" and w[1] == "__interrupt__" for w in tup.pending_writes)
        
        print(f"[{backend.upper()}] Interrupt write latency: {interrupt_latency:.2f} ms")
        print(f"[{backend.upper()}] Interrupt correctly saved: {has_interrupt}")


def main():
    print("Running advanced scenarios...")
    run_high_payload_benchmark()
    run_long_history_benchmark()
    run_time_travel_benchmark()
    run_interrupt_benchmark()
    print("\nAdvanced scenarios complete.")


if __name__ == "__main__":
    main()
