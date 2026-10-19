"""
Módulo del Modelo de Partición.
Representa un fragmento atómico de datos de un RDD distribuido en el clúster.
"""

from dataclasses import dataclass, field
from typing import Optional
from config import PartitionStatus

@dataclass
class Partition:
    """
    Representa una partición individual dentro de un RDD.
    """
    index: int
    rdd_id: int
    size_mb: float
    status: str = PartitionStatus.PENDING
    assigned_worker_id: Optional[str] = None
    is_cached: bool = False
    recomputed_count: int = 0
    records_count: int = 1000

    def mark_computing(self, worker_id: str) -> None:
        self.status = PartitionStatus.COMPUTING
        self.assigned_worker_id = worker_id

    def mark_success(self, cached: bool = False) -> None:
        self.status = PartitionStatus.CACHED_RAM if cached else PartitionStatus.SUCCESS
        self.is_cached = cached

    def mark_lost(self) -> None:
        self.status = PartitionStatus.LOST
        self.is_cached = False
        self.assigned_worker_id = None
        self.recomputed_count += 1

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "rdd_id": self.rdd_id,
            "size_mb": round(self.size_mb, 2),
            "status": self.status,
            "assigned_worker_id": self.assigned_worker_id,
            "is_cached": self.is_cached,
            "recomputed_count": self.recomputed_count,
            "records_count": self.records_count
        }
