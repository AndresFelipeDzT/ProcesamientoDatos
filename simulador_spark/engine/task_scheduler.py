"""
Módulo del Task Scheduler con Cola FIFO.
Asigna tareas listas a núcleos de cómputo disponibles en los WorkerNodes,
respetando el orden FIFO de llegada de trabajos y localidad de datos.
"""

from typing import List, Dict, Optional
from config import TaskStatus, StageStatus, JobStatus, WorkerStatus
from models.worker import WorkerNode
from models.job import Job, Stage, Task

class TaskScheduler:
    """
    Planificador de bajo nivel (FIFO) de Apache Spark.
    Despacha tareas individuales a los Workers del clúster.
    """
    def __init__(self, workers: List[WorkerNode]):
        self.workers: List[WorkerNode] = workers
        self.job_queue: List[Job] = []  # Cola FIFO de Jobs

    def enqueue_job(self, job: Job) -> None:
        """Encola un nuevo trabajo al final de la cola FIFO."""
        self.job_queue.append(job)

    def get_active_job(self) -> Optional[Job]:
        """Obtiene el trabajo actualmente al frente de la cola FIFO que no haya terminado."""
        for job in self.job_queue:
            if job.status in [JobStatus.SUBMITTED, JobStatus.RUNNING]:
                return job
        return None

    def schedule_pending_tasks(self, current_tick: int) -> int:
        """
        Revisa las tareas pendientes del Stage actual y las asigna a Workers con cores libres.
        Retorna la cantidad de tareas asignadas en este ciclo.
        """
        active_job = self.get_active_job()
        if not active_job:
            return 0

        if active_job.status == JobStatus.SUBMITTED:
            active_job.status = JobStatus.RUNNING
            active_job.start_tick = current_tick

        completed_stage_ids = [s.stage_id for s in active_job.stages if s.status == StageStatus.COMPLETED]
        assigned_count = 0

        # Encontrar los stages que están listos para ejecutarse
        for stage in active_job.stages:
            if stage.status == StageStatus.COMPLETED:
                continue

            if not stage.is_ready_to_run(completed_stage_ids):
                # Este stage depende de otros que aún no terminan
                continue

            if stage.status == StageStatus.PENDING:
                stage.status = StageStatus.RUNNING

            # Buscar tareas pendientes en este stage
            pending_tasks = [t for t in stage.tasks if t.status == TaskStatus.PENDING]
            if not pending_tasks:
                # Comprobar si todas las tareas de este stage ya terminaron
                if all(t.status == TaskStatus.SUCCESS for t in stage.tasks):
                    stage.status = StageStatus.COMPLETED
                    completed_stage_ids.append(stage.stage_id)
                continue

            # Asignar cada tarea pendiente a un worker disponible
            for task in pending_tasks:
                best_worker = self._select_best_worker(task)
                if best_worker:
                    assigned = best_worker.assign_task(task)
                    if assigned:
                        assigned_count += 1
                else:
                    # No hay workers con cores libres en este tick
                    break

        # Verificar si el Job completo terminó
        if all(s.status == StageStatus.COMPLETED for s in active_job.stages):
            active_job.status = JobStatus.COMPLETED
            active_job.end_tick = current_tick

        return assigned_count

    def _select_best_worker(self, task: Task) -> Optional[WorkerNode]:
        """
        Selecciona el mejor worker disponible.
        Prioriza localidad de datos (si la partición ya estaba en ese worker)
        o el worker con más cores disponibles para balanceo.
        """
        available_workers = [
            w for w in self.workers
            if w.can_accept_task(task.data_size_mb)
        ]

        if not available_workers:
            return None

        # 1. Chequeo de localidad: ¿algún worker ya tiene esta partición cacheada?
        part_key = f"{task.rdd_id}_{task.partition_index}"
        for w in available_workers:
            if part_key in w.cached_partitions:
                return w

        # 2. Balanceo de carga: seleccionar el worker con mayor cantidad de núcleos libres
        available_workers.sort(key=lambda w: w.available_cores, reverse=True)
        return available_workers[0]
