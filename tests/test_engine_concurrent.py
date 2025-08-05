"""Tests for concurrent execution in DownloadEngine."""

import time
from unittest.mock import patch

from downloader.engine import DownloadEngine


# Mock classes for testing
class MockSlowTaskHandler:
    """A mock task handler that simulates a slow operation."""
    def __init__(self, *args, **kwargs):
        pass

    def execute(self, **kwargs):
        # Simulate a slow task by sleeping
        time.sleep(0.5)


class MockFastTaskHandler:
    """A mock task handler that executes quickly."""
    def __init__(self, *args, **kwargs):
        pass

    def execute(self, **kwargs):
        pass


def create_test_config(task_types):
    """Helper to create a test config with specified task types."""
    tasks = []
    for i, task_type in enumerate(task_types):
        tasks.append({
            "name": f"Test Task {i} ({task_type})",
            "enabled": True,
            "type": task_type,
        })
    return {
        "downloader": {"symbols": ["000001.SZ"]},
        "tasks": tasks,
        "defaults": {},
    }


def test_engine_runs_tasks_sequentially_by_default(
    mock_fetcher, mock_storage, mock_args
):
    """Test that tasks run sequentially by default."""
    config = create_test_config(["slow", "fast", "slow"])
    
    # Registry with our mock handlers
    mock_registry = {
        "slow": MockSlowTaskHandler,
        "fast": MockFastTaskHandler,
    }

    engine = DownloadEngine(config, mock_fetcher, mock_storage, mock_args)

    with patch.object(engine, "task_registry", mock_registry):
        start_time = time.time()
        engine.run()
        end_time = time.time()
        
        # Sequential execution should take approximately 0.5 + 0 + 0.5 = 1 second
        elapsed_time = end_time - start_time
        assert elapsed_time >= 1.0, (
            f"Expected sequential execution to take >= 1s, took {elapsed_time:.2f}s"
        )


def test_engine_runs_tasks_concurrently_when_enabled(
    mock_fetcher, mock_storage, mock_args
):
    """Test that tasks run concurrently when max_workers > 1."""
    config = create_test_config(["slow", "fast", "slow"])
    # Add concurrency config
    config["downloader"]["max_concurrent_tasks"] = 3
    
    # Registry with our mock handlers
    mock_registry = {
        "slow": MockSlowTaskHandler,
        "fast": MockFastTaskHandler,
    }

    engine = DownloadEngine(config, mock_fetcher, mock_storage, mock_args)

    with patch.object(engine, "task_registry", mock_registry):
        start_time = time.time()
        engine.run()
        end_time = time.time()
        
        # Concurrent execution with 3 workers should take approximately 0.5 seconds
        # (as the two 'slow' tasks can run in parallel)
        elapsed_time = end_time - start_time
        assert elapsed_time < 1.0, (
            f"Expected concurrent execution to take < 1s, took {elapsed_time:.2f}s"
        )
        # It should definitely be faster than sequential
        assert elapsed_time < 0.7, (
            f"Expected significant speedup, took {elapsed_time:.2f}s"
        )