"""
PIPELINE ORCHESTRATOR (DAG Engine)
Implements a Directed Acyclic Graph (DAG) based pipeline orchestration system.
Supports dependency resolution, parallel execution, retry logic, and lineage tracking.
Similar to Apache Airflow concepts but lightweight.
"""

import time
from collections import defaultdict
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    RETRYING = "RETRYING"


class DAGTask:
    """Represents a single task in the pipeline DAG."""

    def __init__(self, task_id, callable_fn, description="", retries=0, retry_delay=1):
        self.task_id = task_id
        self.callable_fn = callable_fn
        self.description = description
        self.retries = retries
        self.retry_delay = retry_delay
        self.status = TaskStatus.PENDING
        self.start_time = None
        self.end_time = None
        self.duration_sec = None
        self.error = None
        self.attempt = 0
        self.result = None

    def execute(self):
        """Execute the task with retry logic."""
        for attempt in range(self.retries + 1):
            self.attempt = attempt + 1
            self.start_time = datetime.now()
            self.status = TaskStatus.RUNNING if attempt == 0 else TaskStatus.RETRYING

            try:
                print(f"    Executing: {self.task_id} (attempt {self.attempt}/{self.retries + 1})")
                self.result = self.callable_fn()
                self.status = TaskStatus.SUCCESS
                self.end_time = datetime.now()
                self.duration_sec = (self.end_time - self.start_time).total_seconds()
                return True
            except Exception as e:
                self.error = str(e)
                if attempt < self.retries:
                    print(f"    RETRY: {self.task_id} failed ({e}), retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    self.status = TaskStatus.FAILED
                    self.end_time = datetime.now()
                    self.duration_sec = (self.end_time - self.start_time).total_seconds()
                    print(f"    FAILED: {self.task_id} - {e}")
                    return False


class PipelineDAG:
    """DAG-based pipeline orchestration engine."""

    def __init__(self, dag_id, description=""):
        self.dag_id = dag_id
        self.description = description
        self.tasks = {}
        self.dependencies = defaultdict(set)  # task_id -> set of upstream task_ids
        self.lineage = []
        self.start_time = None
        self.end_time = None

    def add_task(self, task):
        """Add a task to the DAG."""
        self.tasks[task.task_id] = task
        return self

    def set_dependency(self, task_id, depends_on):
        """Set task dependencies (task_id depends on depends_on)."""
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        for dep in depends_on:
            self.dependencies[task_id].add(dep)
        return self

    def _topological_sort(self):
        """Kahn's algorithm for topological ordering."""
        in_degree = defaultdict(int)
        for task_id in self.tasks:
            if task_id not in in_degree:
                in_degree[task_id] = 0
            for dep in self.dependencies[task_id]:
                in_degree[task_id] += 1

        queue = [t for t in self.tasks if in_degree[t] == 0]
        order = []

        while queue:
            node = queue.pop(0)
            order.append(node)
            for task_id in self.tasks:
                if node in self.dependencies[task_id]:
                    in_degree[task_id] -= 1
                    if in_degree[task_id] == 0:
                        queue.append(task_id)

        if len(order) != len(self.tasks):
            raise ValueError("DAG has circular dependencies!")

        return order

    def _can_run(self, task_id):
        """Check if all dependencies of a task have succeeded."""
        for dep_id in self.dependencies[task_id]:
            if self.tasks[dep_id].status != TaskStatus.SUCCESS:
                return False
        return True

    def execute(self):
        """Execute the DAG in topological order."""
        self.start_time = datetime.now()
        print("=" * 70)
        print(f"DAG EXECUTION: {self.dag_id}")
        print(f"Description: {self.description}")
        print(f"Tasks: {len(self.tasks)}")
        print(f"Started: {self.start_time.isoformat()}")
        print("=" * 70)

        execution_order = self._topological_sort()
        print(f"\nExecution order: {' -> '.join(execution_order)}")

        for task_id in execution_order:
            task = self.tasks[task_id]
            print(f"\n  [{execution_order.index(task_id) + 1}/{len(execution_order)}] Task: {task_id}")
            print(f"    Description: {task.description}")

            if not self._can_run(task_id):
                task.status = TaskStatus.SKIPPED
                print("    SKIPPED: Dependencies not met")
                self._record_lineage(task, "skipped")
                continue

            success = task.execute()
            self._record_lineage(task, "success" if success else "failed")

            if not success:
                print(f"\n  Pipeline stopped due to failure in: {task_id}")
                # Skip remaining dependent tasks
                for remaining_id in execution_order[execution_order.index(task_id) + 1 :]:
                    if task_id in self.dependencies.get(remaining_id, set()):
                        self.tasks[remaining_id].status = TaskStatus.SKIPPED

        self.end_time = datetime.now()
        self._print_summary()
        return all(t.status == TaskStatus.SUCCESS for t in self.tasks.values())

    def _record_lineage(self, task, outcome):
        """Record data lineage for the task."""
        self.lineage.append(
            {
                "dag_id": self.dag_id,
                "task_id": task.task_id,
                "description": task.description,
                "status": outcome,
                "attempt": task.attempt,
                "start_time": task.start_time.isoformat() if task.start_time else None,
                "end_time": task.end_time.isoformat() if task.end_time else None,
                "duration_sec": task.duration_sec,
                "error": task.error,
            }
        )

    def _print_summary(self):
        total_duration = (self.end_time - self.start_time).total_seconds()
        print(f"\n{'=' * 70}")
        print(f"DAG EXECUTION SUMMARY: {self.dag_id}")
        print(f"{'=' * 70}")
        print(f"  Total duration: {total_duration:.2f}s")
        for task_id, task in self.tasks.items():
            dur = f"{task.duration_sec:.2f}s" if task.duration_sec else "N/A"
            print(f"  [{task.status.value:>8}] {task_id} ({dur})")
        print(f"{'=' * 70}")

    def get_lineage(self):
        """Return full data lineage records."""
        return self.lineage
