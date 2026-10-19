"""
Módulo del Motor Principal de Simulación de Apache Spark (Simulator).
Coordina los ticks de tiempo discretos, el avance de tareas en los núcleos,
el flujo del planificador FIFO, el manejo de fallos y el registro de telemetría.
"""

from typing import List, Dict, Optional, Any
from config import (
    ClusterConfig, TaskStatus, StageStatus, JobStatus,
    WorkerStatus, DependencyType
)
from models.worker import WorkerNode
from models.driver import DriverNode
from models.rdd import RDD
from models.job import Job, Stage, Task
from engine.dag_scheduler import DAGScheduler
from engine.task_scheduler import TaskScheduler
from engine.fault_manager import FaultManager
from metrics.tracker import MetricsTracker

class SparkSimulator:
    """
    Simulador integral del clúster de Apache Spark.
    """
    def __init__(self, config: ClusterConfig = ClusterConfig()):
        self.config: ClusterConfig = config
        self.current_tick: int = 0
        self.is_running: bool = False

        # 1. Inicializar Driver
        self.driver: DriverNode = DriverNode(
            driver_id="driver-master",
            ram_mb=float(config.DRIVER_RAM_MB)
        )

        # 2. Inicializar Nodos Worker
        self.workers: List[WorkerNode] = []
        for i in range(1, config.NUM_WORKERS + 1):
            worker = WorkerNode(
                worker_id=f"worker-{i}",
                host=f"192.168.1.{100 + i}",
                cores_total=config.CORES_PER_WORKER,
                total_ram_mb=float(config.RAM_PER_WORKER_MB),
                storage_fraction=config.STORAGE_FRACTION
            )
            self.workers.append(worker)

        # 3. Inicializar Componentes del Motor
        self.dag_scheduler: DAGScheduler = DAGScheduler()
        self.task_scheduler: TaskScheduler = TaskScheduler(self.workers)
        self.fault_manager: FaultManager = FaultManager(self.workers, self.driver, self.dag_scheduler)
        self.metrics_tracker: MetricsTracker = MetricsTracker()

        # Contador global de tareas terminadas con éxito
        self.total_completed_tasks: int = 0

    def submit_job(self, target_rdd: RDD, action_name: str, user_id: str, job_name: str) -> Job:
        """
        Punto de entrada cuando un usuario ejecuta una Acción de Spark (Lazy Evaluation -> Ejecución).
        1. El Driver crea el Job.
        2. El DAGScheduler descompone el RDD en Stages y Tasks.
        3. El TaskScheduler encola el Job en la cola FIFO.
        """
        job = self.driver.create_job(
            target_rdd=target_rdd,
            action_name=action_name,
            user_id=user_id,
            job_name=job_name
        )
        job.submitted_tick = self.current_tick

        # Descomponer en Stages (Shuffle Boundaries)
        self.dag_scheduler.create_stages_for_job(job, target_rdd)

        # Encolar en el planificador FIFO
        self.task_scheduler.enqueue_job(job)

        self.fault_manager._log_event(
            tick=self.current_tick,
            event_type="JOB_SUBMITTED",
            message=f"Usuario [{user_id}] envió Job #{job.job_id} ('{job.name}') con {len(job.stages)} Stages.",
            details={"job_id": job.job_id, "action": action_name, "stages": len(job.stages)}
        )

        return job

    def step(self) -> None:
        """
        Avanza la simulación exactamente 1 tick de reloj.
        """
        self.current_tick += 1
        active_job = self.task_scheduler.get_active_job()

        # 1. Avanzar tareas activas en los workers saludables
        for worker in self.workers:
            if worker.status != WorkerStatus.HEALTHY:
                continue

            # Iterar sobre una copia para poder modificar worker.active_tasks al terminar
            for task in list(worker.active_tasks):
                task.elapsed_ticks += 1

                # Verificar si la tarea ha completado su tiempo de cómputo
                if task.elapsed_ticks >= task.total_ticks_needed:
                    task.status = TaskStatus.SUCCESS
                    self.total_completed_tasks += 1
                    worker.release_task(task, success=True)

                    # Obtener el RDD correspondiente a la tarea
                    rdd = self.driver.get_rdd(task.rdd_id)
                    if rdd and task.partition_index < len(rdd.partitions):
                        partition = rdd.partitions[task.partition_index]
                        # Si el RDD tiene caché (.persist() / .cache()), guardarlo en Storage RAM
                        if rdd.is_cached:
                            worker.cache_partition(partition)
                        else:
                            partition.mark_success(cached=False)

                    # Si pertenece a un ShuffleMapStage, sumar Shuffle Write I/O
                    if active_job:
                        for stage in active_job.stages:
                            if stage.stage_id == task.stage_id and stage.is_shuffle_map:
                                stage.shuffle_write_mb += task.data_size_mb

        # 2. Asignar tareas pendientes según la cola FIFO a los núcleos liberados
        self.task_scheduler.schedule_pending_tasks(self.current_tick)

        # 3. Detectar si el Job activo terminó en este tick
        if active_job and active_job.status == JobStatus.COMPLETED and active_job.end_tick == self.current_tick:
            self.metrics_tracker.record_job_completed(active_job, self.current_tick)
            self.fault_manager._log_event(
                tick=self.current_tick,
                event_type="JOB_COMPLETED",
                message=f"Job #{active_job.job_id} finalizado exitosamente en {active_job.duration_ticks(self.current_tick)} ticks.",
                details={"job_id": active_job.job_id, "duration": active_job.duration_ticks(self.current_tick)}
            )

        # 4. Registrar telemetría del clúster
        self.metrics_tracker.record_tick(self.current_tick, self.workers, self.total_completed_tasks)

    def kill_worker(self, worker_id: str) -> bool:
        """Inyecta un fallo en un nodo."""
        active_job = self.task_scheduler.get_active_job()
        return self.fault_manager.kill_worker(worker_id, self.current_tick, active_job)

    def revive_worker(self, worker_id: str) -> bool:
        """Restaura un nodo caído."""
        return self.fault_manager.revive_worker(worker_id, self.current_tick)

    def get_cluster_state(self) -> Dict[str, Any]:
        """
        Retorna una instantánea completa del estado del clúster serializable a JSON
        para la interfaz web y APIs.
        """
        active_job = self.task_scheduler.get_active_job()
        summary = self.metrics_tracker.get_summary(self.workers)

        return {
            "current_tick": self.current_tick,
            "is_running": self.is_running,
            "driver": self.driver.to_dict(),
            "workers": [w.to_dict() for w in self.workers],
            "active_job": active_job.to_dict(self.current_tick) if active_job else None,
            "queue": [j.to_dict(self.current_tick) for j in self.task_scheduler.job_queue if j.status == JobStatus.SUBMITTED],
            "completed_jobs": self.metrics_tracker.completed_jobs_metrics,
            "metrics": summary,
            "events": self.fault_manager.get_recent_events(limit=12),
            "resilience": {
                "total_node_failures": self.fault_manager.total_node_failures,
                "total_tasks_retried": self.fault_manager.total_tasks_retried,
                "total_partitions_recomputed": self.fault_manager.total_partitions_recomputed
            }
        }
