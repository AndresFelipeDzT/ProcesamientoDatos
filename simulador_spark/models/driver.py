"""
Módulo del Nodo Driver (Spark Driver / SparkSession).
Actúa como el coordinador central de la aplicación Spark. Mantiene el catálogo
de RDDs, recibe las acciones de los usuarios y coordina la ejecución con los schedulers.
"""

from typing import Dict, List, Optional, Any
from models.rdd import RDD
from models.job import Job
from config import JobStatus

class DriverNode:
    """
    Representa el proceso Driver de Apache Spark.
    """
    def __init__(self, driver_id: str = "driver-master", ram_mb: float = 2048.0):
        self.driver_id: str = driver_id
        self.ram_mb: float = ram_mb
        self.rdds: Dict[int, RDD] = {}
        self.jobs: Dict[int, Job] = {}
        self._job_counter: int = 0

    def parallelize(
        self,
        name: str = "data_source",
        num_partitions: int = 4,
        total_data_size_mb: float = 100.0,
        skew_factor: float = 1.0
    ) -> RDD:
        """
        Crea un RDD distribuido inicial a partir de una fuente de datos.
        """
        rdd = RDD(
            name=name,
            operation="parallelize",
            num_partitions=num_partitions,
            parents=[],
            total_data_size_mb=total_data_size_mb,
            skew_factor=skew_factor
        )
        self.register_rdd(rdd)
        return rdd

    def register_rdd(self, rdd: RDD) -> None:
        """Registra un RDD y sus ancestros en el catálogo del Driver."""
        lineage = rdd.get_lineage()
        for node in lineage:
            self.rdds[node.id] = node

    def create_job(self, target_rdd: RDD, action_name: str, user_id: str, job_name: str) -> Job:
        """
        Crea un nuevo Job cuando el usuario invoca una Acción sobre un RDD.
        """
        self._job_counter += 1
        self.register_rdd(target_rdd)

        job = Job(
            job_id=self._job_counter,
            name=job_name,
            user_id=user_id,
            action_name=action_name,
            target_rdd_id=target_rdd.id,
            status=JobStatus.SUBMITTED
        )
        self.jobs[job.job_id] = job
        return job

    def get_job(self, job_id: int) -> Optional[Job]:
        return self.jobs.get(job_id)

    def get_rdd(self, rdd_id: int) -> Optional[RDD]:
        return self.rdds.get(rdd_id)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "driver_id": self.driver_id,
            "ram_mb": self.ram_mb,
            "registered_rdds_count": len(self.rdds),
            "total_jobs_count": len(self.jobs),
            "active_jobs_count": sum(1 for j in self.jobs.values() if j.status == JobStatus.RUNNING),
            "completed_jobs_count": sum(1 for j in self.jobs.values() if j.status == JobStatus.COMPLETED)
        }
