"""
Módulo de Modelos de Ejecución Física: Job, Stage y Task.
Representa cómo Spark traduce un DAG lógico de RDDs en unidades ejecutables físicas.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from config import TaskStatus, StageStatus, JobStatus
from models.rdd import RDD

@dataclass
class Task:
    """
    Unidad atómica de trabajo en Spark. Se ejecuta en un único core de un Worker
    sobre una única partición de un RDD dentro de un Stage.
    """
    task_id: int
    stage_id: int
    partition_index: int
    rdd_id: int
    data_size_mb: float
    total_ticks_needed: int = 10
    elapsed_ticks: int = 0
    worker_id: Optional[str] = None
    status: str = TaskStatus.PENDING
    attempt: int = 1
    is_recomputation: bool = False
    error_message: Optional[str] = None

    def progress_pct(self) -> float:
        if self.total_ticks_needed == 0:
            return 100.0
        return min(100.0, round((self.elapsed_ticks / self.total_ticks_needed) * 100, 1))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "stage_id": self.stage_id,
            "partition_index": self.partition_index,
            "rdd_id": self.rdd_id,
            "data_size_mb": round(self.data_size_mb, 2),
            "total_ticks_needed": self.total_ticks_needed,
            "elapsed_ticks": self.elapsed_ticks,
            "worker_id": self.worker_id,
            "status": self.status,
            "attempt": self.attempt,
            "is_recomputation": self.is_recomputation,
            "progress_pct": self.progress_pct()
        }

@dataclass
class Stage:
    """
    Conjunto de tareas que pueden ejecutarse en paralelo sin necesidad de Shuffle en red.
    Un límite de Stage ocurre cada vez que hay una dependencia WIDE (Shuffle).
    """
    stage_id: int
    job_id: int
    name: str
    is_shuffle_map: bool
    parent_stage_ids: List[int] = field(default_factory=list)
    rdd_ids: List[int] = field(default_factory=list)
    tasks: List[Task] = field(default_factory=list)
    status: str = StageStatus.PENDING
    shuffle_write_mb: float = 0.0
    shuffle_read_mb: float = 0.0

    def is_ready_to_run(self, completed_stage_ids: List[int]) -> bool:
        """Un Stage solo puede arrancar cuando todos sus padres han terminado."""
        return all(p_id in completed_stage_ids for p_id in self.parent_stage_ids)

    def is_completed(self) -> bool:
        if not self.tasks:
            return False
        return all(t.status == TaskStatus.SUCCESS for t in self.tasks)

    def to_dict(self) -> Dict[str, Any]:
        completed_tasks = sum(1 for t in self.tasks if t.status == TaskStatus.SUCCESS)
        total_tasks = len(self.tasks)
        return {
            "stage_id": self.stage_id,
            "job_id": self.job_id,
            "name": self.name,
            "is_shuffle_map": self.is_shuffle_map,
            "parent_stage_ids": self.parent_stage_ids,
            "rdd_ids": self.rdd_ids,
            "status": self.status,
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "progress_pct": round((completed_tasks / max(1, total_tasks)) * 100, 1),
            "shuffle_write_mb": round(self.shuffle_write_mb, 2),
            "shuffle_read_mb": round(self.shuffle_read_mb, 2),
            "tasks": [t.to_dict() for t in self.tasks]
        }

@dataclass
class Job:
    """
    Representa una aplicación o consulta disparada por una Acción de Spark.
    Contiene uno o más Stages.
    """
    job_id: int
    name: str
    user_id: str
    action_name: str
    target_rdd_id: int
    stages: List[Stage] = field(default_factory=list)
    status: str = JobStatus.SUBMITTED
    submitted_tick: int = 0
    start_tick: Optional[int] = None
    end_tick: Optional[int] = None

    def is_completed(self) -> bool:
        return bool(self.stages) and all(s.status == StageStatus.COMPLETED for s in self.stages)

    def duration_ticks(self, current_tick: int) -> int:
        if self.start_tick is None:
            return 0
        if self.end_tick is not None:
            return self.end_tick - self.start_tick
        return current_tick - self.start_tick

    def to_dict(self, current_tick: int = 0) -> Dict[str, Any]:
        total_tasks = sum(len(s.tasks) for s in self.stages)
        completed_tasks = sum(sum(1 for t in s.tasks if t.status == TaskStatus.SUCCESS) for s in self.stages)
        failed_tasks = sum(sum(1 for t in s.tasks if t.status == TaskStatus.FAILED) for s in self.stages)
        recomputed_tasks = sum(sum(1 for t in s.tasks if t.is_recomputation) for s in self.stages)

        return {
            "job_id": self.job_id,
            "name": self.name,
            "user_id": self.user_id,
            "action_name": self.action_name,
            "target_rdd_id": self.target_rdd_id,
            "status": self.status,
            "submitted_tick": self.submitted_tick,
            "start_tick": self.start_tick,
            "end_tick": self.end_tick,
            "duration_ticks": self.duration_ticks(current_tick),
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "recomputed_tasks": recomputed_tasks,
            "progress_pct": round((completed_tasks / max(1, total_tasks)) * 100, 1) if total_tasks > 0 else 0.0,
            "stages": [s.to_dict() for s in self.stages]
        }
