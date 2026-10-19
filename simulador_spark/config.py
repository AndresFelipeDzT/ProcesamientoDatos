"""
Módulo de Configuración Global del Simulador de Apache Spark.
Define constantes del clúster, políticas de memoria y parámetros de simulación.
"""

from dataclasses import dataclass
from typing import Dict, Any

# ==========================================
# Constantes de Estado y Tipos de Dependencia
# ==========================================

class DependencyType:
    NARROW = "NARROW"  # map, filter: 1 partición padre -> 1 partición hija (Sin Shuffle)
    WIDE = "WIDE"      # groupByKey, reduceByKey: muchas particiones -> muchas (Requiere Shuffle)

class PartitionStatus:
    PENDING = "PENDING"
    COMPUTING = "COMPUTING"
    CACHED_RAM = "CACHED_RAM"
    LOST = "LOST"
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"

class TaskStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"

class StageStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class JobStatus:
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class WorkerStatus:
    HEALTHY = "HEALTHY"
    DEAD = "DEAD"
    DEGRADED = "DEGRADED"

# ==========================================
# Parámetros Predeterminados del Clúster
# ==========================================

@dataclass(frozen=True)
class ClusterConfig:
    NUM_WORKERS: int = 4
    CORES_PER_WORKER: int = 4
    RAM_PER_WORKER_MB: int = 4096       # 4 GB por Worker
    STORAGE_FRACTION: float = 0.5       # 50% RAM para Caché de RDDs, 50% para ejecución/shuffle
    DRIVER_RAM_MB: int = 2048           # 2 GB para el Driver
    NETWORK_BANDWIDTH_MBPS: float = 100 # Ancho de banda de Shuffle entre workers
    TICK_DURATION_MS: int = 100         # Duración de cada tick de simulación
    HEARTBEAT_TIMEOUT_TICKS: int = 10   # Ticks para considerar un worker como muerto si no envía latido
