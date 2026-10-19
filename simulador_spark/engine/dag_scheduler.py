"""
Módulo del DAG Scheduler de Apache Spark.
Traduce el grafo de linaje de un RDD en un conjunto de Stages físicos,
identificando fronteras de Shuffle (Narrow vs Wide) y creando las Tasks correspondientes.
"""

from typing import List, Dict, Set, Optional
from config import DependencyType, StageStatus, TaskStatus
from models.rdd import RDD
from models.job import Job, Stage, Task

class DAGScheduler:
    """
    Planificador de alto nivel en Spark. Responsable de:
    1. Construir Stages divididos por dependencias WIDE (Shuffle).
    2. Pipelining de dependencias NARROW dentro del mismo Stage.
    3. Manejo de reconstrucción de particiones perdidas mediante Linaje.
    """
    def __init__(self):
        self._stage_id_counter: int = 0
        self._task_id_counter: int = 0

    def create_stages_for_job(self, job: Job, target_rdd: RDD) -> List[Stage]:
        """
        Divide el grafo de RDDs en Stages examinando las dependencias hacia atrás.
        """
        visited_rdds: Set[int] = set()
        stages: List[Stage] = []
        rdd_to_stage: Dict[int, Stage] = {}

        def _build_stage(current_rdd: RDD, is_result_stage: bool) -> Stage:
            self._stage_id_counter += 1
            stage_id = self._stage_id_counter
            stage_name = f"Stage-{stage_id} ({'Result' if is_result_stage else 'ShuffleMap'}: {current_rdd.operation})"

            stage = Stage(
                stage_id=stage_id,
                job_id=job.job_id,
                name=stage_name,
                is_shuffle_map=not is_result_stage,
                rdd_ids=[current_rdd.id]
            )

            # Recorrer ancestros para agrupar transformaciones Narrow
            queue = [current_rdd]
            while queue:
                rdd = queue.pop(0)
                visited_rdds.add(rdd.id)
                rdd_to_stage[rdd.id] = stage

                for parent in rdd.parents:
                    if parent.dependency_type == DependencyType.WIDE:
                        # Frontera de Shuffle: El padre debe pertenecer a un Stage previo
                        if parent.id not in visited_rdds:
                            parent_stage = _build_stage(parent, is_result_stage=False)
                            stages.append(parent_stage)
                        else:
                            parent_stage = rdd_to_stage[parent.id]

                        if parent_stage.stage_id not in stage.parent_stage_ids:
                            stage.parent_stage_ids.append(parent_stage.stage_id)
                    else:
                        # Dependencia NARROW: Se fusiona en el mismo Stage (Pipelining)
                        if parent.id not in stage.rdd_ids:
                            stage.rdd_ids.append(parent.id)
                        queue.append(parent)

            # Generar las tareas para este Stage (1 tarea por partición del RDD objetivo del stage)
            stage.tasks = self._create_tasks_for_stage(stage, current_rdd)
            return stage

        # El Stage final del Job es siempre el ResultStage
        final_stage = _build_stage(target_rdd, is_result_stage=True)
        stages.append(final_stage)

        # Ordenar etapas topológicamente (los padres primero)
        stages_sorted = self._topological_sort_stages(stages)
        job.stages = stages_sorted
        return stages_sorted

    def _create_tasks_for_stage(self, stage: Stage, rdd: RDD) -> List[Task]:
        """Crea una tarea por cada partición del RDD que produce el Stage."""
        tasks = []
        for partition in rdd.partitions:
            self._task_id_counter += 1
            # El tiempo necesario depende del tamaño de los datos (1 tick cada ~10 MB)
            ticks_needed = max(3, int(partition.size_mb / 10.0))
            task = Task(
                task_id=self._task_id_counter,
                stage_id=stage.stage_id,
                partition_index=partition.index,
                rdd_id=rdd.id,
                data_size_mb=partition.size_mb,
                total_ticks_needed=ticks_needed,
                status=TaskStatus.PENDING
            )
            tasks.append(task)
        return tasks

    def create_recomputation_tasks_for_partitions(
        self,
        stage: Stage,
        rdd: RDD,
        lost_partition_indices: List[int]
    ) -> List[Task]:
        """
        RECÁLCULO POR LINAJE (Resilience):
        Genera tareas ÚNICAMENTE para las particiones que se perdieron debido a la caída de un Worker.
        """
        recovery_tasks = []
        for p_idx in lost_partition_indices:
            partition = rdd.partitions[p_idx]
            self._task_id_counter += 1
            ticks_needed = max(3, int(partition.size_mb / 10.0))
            task = Task(
                task_id=self._task_id_counter,
                stage_id=stage.stage_id,
                partition_index=p_idx,
                rdd_id=rdd.id,
                data_size_mb=partition.size_mb,
                total_ticks_needed=ticks_needed,
                status=TaskStatus.PENDING,
                is_recomputation=True,
                attempt=2
            )
            recovery_tasks.append(task)
        return recovery_tasks

    def _topological_sort_stages(self, stages: List[Stage]) -> List[Stage]:
        """Ordena los Stages de manera que los que no tienen dependencias pendientes vayan primero."""
        stage_map = {s.stage_id: s for s in stages}
        visited = set()
        result = []

        def visit(s: Stage):
            if s.stage_id not in visited:
                visited.add(s.stage_id)
                for pid in s.parent_stage_ids:
                    if pid in stage_map:
                        visit(stage_map[pid])
                result.append(s)

        for s in stages:
            visit(s)

        return result
