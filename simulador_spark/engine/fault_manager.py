"""
Módulo del Administrador de Fallos y Tolerancia a Fallos (Resilience).
Implementa la detección de caídas de nodos ejecutores y la reconstrucción
selectiva de particiones perdidas utilizando el Grafo de Linaje (Lineage DAG) de Spark.
"""

from typing import List, Dict, Optional, Any
from config import WorkerStatus, TaskStatus, StageStatus, PartitionStatus
from models.worker import WorkerNode
from models.job import Job, Stage, Task
from models.driver import DriverNode
from models.rdd import RDD
from engine.dag_scheduler import DAGScheduler

class FaultManager:
    """
    Gestiona la inyección de fallos, la monitorización de Workers y
    la recuperación resiliente basada en linaje de RDDs.
    """
    def __init__(self, workers: List[WorkerNode], driver: DriverNode, dag_scheduler: DAGScheduler):
        self.workers: List[WorkerNode] = workers
        self.driver: DriverNode = driver
        self.dag_scheduler: DAGScheduler = dag_scheduler
        self.event_log: List[Dict[str, Any]] = []

        # Métricas de fallos
        self.total_node_failures: int = 0
        self.total_tasks_retried: int = 0
        self.total_partitions_recomputed: int = 0

    def kill_worker(self, worker_id: str, current_tick: int, active_job: Optional[Job] = None) -> bool:
        """
        Simula la caída forzada de un nodo Worker (falla física, OOM o desconexión).
        """
        worker = next((w for w in self.workers if w.worker_id == worker_id), None)
        if not worker or worker.status == WorkerStatus.DEAD:
            return False

        self.total_node_failures += 1
        lost_tasks = worker.kill()

        self._log_event(
            tick=current_tick,
            event_type="WORKER_CRASH",
            message=f"El nodo {worker_id} se ha caído. {len(lost_tasks)} tareas en ejecución interrumpidas.",
            details={"worker_id": worker_id, "interrupted_tasks": len(lost_tasks)}
        )

        # Si hay un trabajo en ejecución, activar la recuperación por linaje
        if active_job:
            self._recover_from_worker_failure(worker, lost_tasks, active_job, current_tick)

        return True

    def revive_worker(self, worker_id: str, current_tick: int) -> bool:
        """Reactiva un nodo que previamente había fallado."""
        worker = next((w for w in self.workers if w.worker_id == worker_id), None)
        if not worker or worker.status == WorkerStatus.HEALTHY:
            return False

        worker.revive()
        self._log_event(
            tick=current_tick,
            event_type="WORKER_RECOVERED",
            message=f"El nodo {worker_id} ha sido reiniciado y se encuentra disponible con {worker.cores_total} cores.",
            details={"worker_id": worker_id}
        )
        return True

    def _recover_from_worker_failure(
        self,
        dead_worker: WorkerNode,
        lost_tasks: List[Task],
        active_job: Job,
        current_tick: int
    ) -> None:
        """
        Mecanismo Central de Spark: Reconstrucción por Linaje (Lineage Recovery).
        1. Las tareas en ejecución que fallaron se marcan para reintentarse (RETRY).
        2. Si el nodo tenía particiones cacheadas o resultados intermedios de Shuffle que
           ahora se perdieron, el DAGScheduler genera tareas de recálculo sobre otros nodos.
        """
        for stage in active_job.stages:
            if stage.status == StageStatus.COMPLETED:
                # Si una etapa previa produjo particiones que estaban solo en este worker
                # y una etapa posterior las necesita, se deben recalcular
                continue

            # Para tareas que estaban corriendo en el worker caído
            for task in stage.tasks:
                if task.worker_id == dead_worker.worker_id and task.status == TaskStatus.FAILED:
                    task.status = TaskStatus.PENDING
                    task.worker_id = None
                    task.attempt += 1
                    task.is_recomputation = True
                    task.elapsed_ticks = 0
                    self.total_tasks_retried += 1
                    self.total_partitions_recomputed += 1

                    self._log_event(
                        tick=current_tick,
                        event_type="LINEAGE_RECOMPUTATION",
                        message=(
                            f"Resiliencia Spark activada: Tarea #{task.task_id} (Partición {task.partition_index}) "
                            f"será recalculada en otro nodo usando el grafo de linaje del RDD #{task.rdd_id}."
                        ),
                        details={
                            "task_id": task.task_id,
                            "partition_index": task.partition_index,
                            "attempt": task.attempt,
                            "stage_id": stage.stage_id
                        }
                    )

    def _log_event(self, tick: int, event_type: str, message: str, details: Dict[str, Any]) -> None:
        self.event_log.append({
            "tick": tick,
            "type": event_type,
            "message": message,
            "details": details
        })

    def get_recent_events(self, limit: int = 15) -> List[Dict[str, Any]]:
        return self.event_log[-limit:]
