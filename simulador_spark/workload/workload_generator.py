"""
Módulo del Generador de Carga de Trabajo (Workload Generator).
Permite inyectar trabajos individuales o ráfagas concurrentes de múltiples usuarios
para saturar la cola FIFO y los núcleos del clúster.
"""

from typing import List, Optional
from engine.simulator import SparkSimulator
from workload.user_profiles import (
    create_interactive_analyst_job,
    create_etl_pipeline_job,
    create_ml_training_job
)
from models.job import Job

class WorkloadGenerator:
    """
    Controlador de generación de tráfico y usuarios concurrentes.
    """
    def __init__(self, simulator: SparkSimulator):
        self.simulator: SparkSimulator = simulator

    def submit_analyst_query(self, user_id: str = "analyst_maria", data_size_mb: float = 80.0) -> Job:
        """Envía una consulta interactiva rápida."""
        target_rdd, action, uid, job_name = create_interactive_analyst_job(
            self.simulator.driver, user_id=user_id, data_size_mb=data_size_mb
        )
        return self.simulator.submit_job(target_rdd, action, uid, job_name)

    def submit_etl_pipeline(
        self,
        user_id: str = "engineer_carlos",
        data_size_mb: float = 350.0,
        num_partitions: int = 8,
        skew_factor: float = 1.0
    ) -> Job:
        """Envía un pipeline ETL por lotes con Shuffle en red."""
        target_rdd, action, uid, job_name = create_etl_pipeline_job(
            self.simulator.driver,
            user_id=user_id,
            data_size_mb=data_size_mb,
            num_partitions=num_partitions,
            skew_factor=skew_factor
        )
        return self.simulator.submit_job(target_rdd, action, uid, job_name)

    def submit_ml_pipeline(self, user_id: str = "scientist_laura") -> Job:
        """Envía un trabajo de Machine Learning con persistencia en memoria RAM (.persist())."""
        target_rdd, action, uid, job_name = create_ml_training_job(
            self.simulator.driver, user_id=user_id
        )
        return self.simulator.submit_job(target_rdd, action, uid, job_name)

    def submit_burst_workload(self) -> List[Job]:
        """
        Envía una ráfaga simultánea de trabajos de diferentes usuarios
        para demostrar el encolamiento en orden FIFO y la contención de núcleos.
        """
        j1 = self.submit_analyst_query("analyst_pedro", data_size_mb=60.0)
        j2 = self.submit_etl_pipeline("engineer_sofia", data_size_mb=250.0, num_partitions=6)
        j3 = self.submit_ml_pipeline("scientist_andres")
        return [j1, j2, j3]
