"""
Módulo de Métricas y Telemetría del Clúster Spark.
Calcula y almacena series de tiempo para CPU, RAM, I/O de Shuffle,
latencias de trabajos y métricas de tolerancia a fallos.
"""

from typing import List, Dict, Any
from models.worker import WorkerNode
from models.job import Job
from config import JobStatus

class MetricsTracker:
    """
    Rastreador de telemetría y KPIs en tiempo real del simulador.
    """
    def __init__(self):
        # Series de tiempo (historial por tick)
        self.tick_history: List[int] = []
        self.cpu_history: List[float] = []           # CPU promedio del clúster (%)
        self.ram_history: List[float] = []           # RAM total usada (MB)
        self.active_tasks_history: List[int] = []    # Tareas en ejecución
        self.completed_tasks_history: List[int] = [] # Tareas finalizadas acumuladas

        # Métricas agregadas
        self.total_shuffle_bytes_mb: float = 0.0
        self.completed_jobs_metrics: List[Dict[str, Any]] = []

    def record_tick(self, tick: int, workers: List[WorkerNode], total_completed_tasks: int) -> None:
        """Registra una instantánea de métricas en el tick actual."""
        if not workers:
            return

        active_workers = [w for w in workers if w.status != "DEAD"]
        avg_cpu = sum(w.cpu_utilization_pct() for w in active_workers) / max(1, len(active_workers))
        total_ram_used = sum(w.used_execution_ram_mb + w.used_storage_ram_mb for w in active_workers)
        active_tasks = sum(len(w.active_tasks) for w in active_workers)

        self.tick_history.append(tick)
        self.cpu_history.append(round(avg_cpu, 1))
        self.ram_history.append(round(total_ram_used, 1))
        self.active_tasks_history.append(active_tasks)
        self.completed_tasks_history.append(total_completed_tasks)

        # Mantener un límite razonable en memoria para el historial gráfico (últimos 150 ticks)
        if len(self.tick_history) > 150:
            self.tick_history.pop(0)
            self.cpu_history.pop(0)
            self.ram_history.pop(0)
            self.active_tasks_history.pop(0)
            self.completed_tasks_history.pop(0)

    def record_job_completed(self, job: Job, current_tick: int) -> None:
        """Registra estadísticas al finalizar un trabajo."""
        duration = job.duration_ticks(current_tick)
        total_tasks = sum(len(s.tasks) for s in job.stages)
        recomputed = sum(sum(1 for t in s.tasks if t.is_recomputation) for s in job.stages)
        shuffle_mb = sum(s.shuffle_write_mb for s in job.stages)

        self.total_shuffle_bytes_mb += shuffle_mb
        self.completed_jobs_metrics.append({
            "job_id": job.job_id,
            "job_name": job.name,
            "user_id": job.user_id,
            "duration_ticks": duration,
            "total_tasks": total_tasks,
            "recomputed_tasks": recomputed,
            "shuffle_mb": round(shuffle_mb, 2),
            "end_tick": current_tick
        })

    def get_summary(self, workers: List[WorkerNode]) -> Dict[str, Any]:
        """Devuelve un resumen general de las métricas clave."""
        total_cores = sum(w.cores_total for w in workers)
        used_cores = sum(w.cores_used for w in workers if w.status != "DEAD")
        total_ram = sum(w.total_ram_mb for w in workers)
        used_ram = sum(w.used_execution_ram_mb + w.used_storage_ram_mb for w in workers if w.status != "DEAD")

        avg_job_latency = (
            sum(j["duration_ticks"] for j in self.completed_jobs_metrics) / max(1, len(self.completed_jobs_metrics))
            if self.completed_jobs_metrics else 0.0
        )

        return {
            "cluster_cores_total": total_cores,
            "cluster_cores_used": used_cores,
            "cluster_cores_utilization_pct": round((used_cores / max(1, total_cores)) * 100, 1),
            "cluster_ram_total_mb": total_ram,
            "cluster_ram_used_mb": round(used_ram, 1),
            "cluster_ram_utilization_pct": round((used_ram / max(1, total_ram)) * 100, 1),
            "total_shuffle_mb": round(self.total_shuffle_bytes_mb, 2),
            "completed_jobs_count": len(self.completed_jobs_metrics),
            "average_job_duration_ticks": round(avg_job_latency, 1),
            "history": {
                "ticks": self.tick_history,
                "cpu": self.cpu_history,
                "ram": self.ram_history,
                "active_tasks": self.active_tasks_history,
                "completed_tasks": self.completed_tasks_history
            }
        }
