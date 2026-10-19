"""
Módulo del Modelo de Nodo Worker (Ejecutor).
Representa una máquina física o contenedor en el clúster con cores de CPU y
gestión diferenciada de memoria RAM (Execution vs Storage).
"""

from typing import List, Dict, Optional, Any
from config import WorkerStatus, TaskStatus
from models.job import Task
from models.partition import Partition

class WorkerNode:
    """
    Representa un Worker/Executor en el clúster de Spark.
    """
    def __init__(
        self,
        worker_id: str,
        host: str,
        cores_total: int = 4,
        total_ram_mb: float = 4096.0,
        storage_fraction: float = 0.5
    ):
        self.worker_id: str = worker_id
        self.host: str = host
        self.cores_total: int = cores_total
        self.cores_used: int = 0
        self.total_ram_mb: float = total_ram_mb
        self.max_storage_ram_mb: float = total_ram_mb * storage_fraction
        self.max_execution_ram_mb: float = total_ram_mb * (1.0 - storage_fraction)

        # Memoria en uso actual
        self.used_storage_ram_mb: float = 0.0
        self.used_execution_ram_mb: float = 0.0

        self.status: str = WorkerStatus.HEALTHY
        self.active_tasks: List[Task] = []
        self.cached_partitions: Dict[str, Partition] = {}  # "rddId_partIdx" -> Partition

        self.completed_tasks_count: int = 0
        self.failed_tasks_count: int = 0
        self.consecutive_heartbeats_missed: int = 0

    @property
    def available_cores(self) -> int:
        if self.status != WorkerStatus.HEALTHY:
            return 0
        return max(0, self.cores_total - self.cores_used)

    @property
    def free_execution_ram_mb(self) -> float:
        if self.status != WorkerStatus.HEALTHY:
            return 0.0
        return max(0.0, self.max_execution_ram_mb - self.used_execution_ram_mb)

    def can_accept_task(self, ram_needed_mb: float = 100.0) -> bool:
        return (
            self.status == WorkerStatus.HEALTHY
            and self.available_cores > 0
            and self.free_execution_ram_mb >= ram_needed_mb
        )

    def assign_task(self, task: Task) -> bool:
        if not self.can_accept_task(task.data_size_mb):
            return False

        task.worker_id = self.worker_id
        task.status = TaskStatus.RUNNING
        self.active_tasks.append(task)
        self.cores_used += 1
        self.used_execution_ram_mb += task.data_size_mb
        return True

    def release_task(self, task: Task, success: bool = True) -> None:
        if task in self.active_tasks:
            self.active_tasks.remove(task)
            self.cores_used = max(0, self.cores_used - 1)
            self.used_execution_ram_mb = max(0.0, self.used_execution_ram_mb - task.data_size_mb)

            if success:
                self.completed_tasks_count += 1
            else:
                self.failed_tasks_count += 1

    def cache_partition(self, partition: Partition) -> bool:
        """Almacena una partición en la memoria de almacenamiento del worker (Storage)."""
        key = f"{partition.rdd_id}_{partition.index}"
        if self.used_storage_ram_mb + partition.size_mb <= self.max_storage_ram_mb:
            self.cached_partitions[key] = partition
            self.used_storage_ram_mb += partition.size_mb
            partition.mark_success(cached=True)
            return True
        # Si no cabe, no se cachea en RAM (se mantiene calculada sin persistencia en memoria)
        partition.mark_success(cached=False)
        return False

    def kill(self) -> List[Task]:
        """Simula una caída de nodo (falla de hardware/red)."""
        self.status = WorkerStatus.DEAD
        lost_tasks = list(self.active_tasks)
        self.active_tasks.clear()
        self.cores_used = 0
        self.used_execution_ram_mb = 0.0

        # Al morir el nodo, se pierden las particiones en RAM (volatilidad de la memoria)
        for part in self.cached_partitions.values():
            part.mark_lost()
        self.cached_partitions.clear()
        self.used_storage_ram_mb = 0.0

        for t in lost_tasks:
            t.status = TaskStatus.FAILED
            t.error_message = f"ExecutorLostFailure: Worker {self.worker_id} crashed."
            self.failed_tasks_count += 1

        return lost_tasks

    def revive(self) -> None:
        """Restaura el nodo a estado saludable."""
        self.status = WorkerStatus.HEALTHY
        self.consecutive_heartbeats_missed = 0
        self.cores_used = 0
        self.active_tasks.clear()
        self.used_execution_ram_mb = 0.0
        self.used_storage_ram_mb = 0.0
        self.cached_partitions.clear()

    def cpu_utilization_pct(self) -> float:
        if self.status != WorkerStatus.HEALTHY or self.cores_total == 0:
            return 0.0
        return round((self.cores_used / self.cores_total) * 100, 1)

    def ram_utilization_pct(self) -> float:
        if self.status != WorkerStatus.HEALTHY or self.total_ram_mb == 0:
            return 0.0
        total_used = self.used_execution_ram_mb + self.used_storage_ram_mb
        return round((total_used / self.total_ram_mb) * 100, 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "host": self.host,
            "status": self.status,
            "cores_total": self.cores_total,
            "cores_used": self.cores_used,
            "available_cores": self.available_cores,
            "cpu_utilization_pct": self.cpu_utilization_pct(),
            "total_ram_mb": self.total_ram_mb,
            "used_execution_ram_mb": round(self.used_execution_ram_mb, 1),
            "used_storage_ram_mb": round(self.used_storage_ram_mb, 1),
            "total_used_ram_mb": round(self.used_execution_ram_mb + self.used_storage_ram_mb, 1),
            "ram_utilization_pct": self.ram_utilization_pct(),
            "cached_partitions_count": len(self.cached_partitions),
            "active_tasks_count": len(self.active_tasks),
            "completed_tasks_count": self.completed_tasks_count,
            "failed_tasks_count": self.failed_tasks_count
        }
