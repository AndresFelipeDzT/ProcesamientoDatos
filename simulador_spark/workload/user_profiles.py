"""
Módulo de Perfiles de Usuario y Plantillas de Carga.
Alineado directamente con los roles de Big Data de las diapositivas del curso:
1. Business / Data Analyst: Consultas interactivas rápidas (Narrow, baja latencia).
2. Data Engineer: Pipelines pesados de ETL y agregación masiva (Wide, Shuffle intenso).
3. Data Scientist / ML Engineer: Cómputo iterativo con persistencia en RAM (.cache()).
"""

from typing import Tuple
from models.driver import DriverNode
from models.rdd import RDD
from models.job import Job

class UserProfile:
    BUSINESS_ANALYST = "Business_Analyst"
    DATA_ENGINEER = "Data_Engineer"
    DATA_SCIENTIST = "Data_Scientist"

def create_interactive_analyst_job(
    driver: DriverNode,
    user_id: str = "analyst_maria",
    data_size_mb: float = 80.0,
    num_partitions: int = 4
) -> Tuple[RDD, str, str, str]:
    """
    Trabajo tipo Business Analyst:
    Carga de eventos -> filtro ligero -> Acción count().
    Genera un único Stage (solo dependencias Narrow, sin Shuffle).
    """
    raw_rdd = driver.parallelize(
        name="web_traffic_logs",
        num_partitions=num_partitions,
        total_data_size_mb=data_size_mb
    )
    filtered_rdd = raw_rdd.filter(name="filter(status_code == 200)")

    action_name = "count()"
    job_name = "Analyst Query: Count HTTP 200 OK"
    return filtered_rdd, action_name, user_id, job_name

def create_etl_pipeline_job(
    driver: DriverNode,
    user_id: str = "engineer_carlos",
    data_size_mb: float = 350.0,
    num_partitions: int = 8,
    skew_factor: float = 1.0
) -> Tuple[RDD, str, str, str]:
    """
    Trabajo tipo Data Engineer (Batch ETL):
    Lectura de transacciones -> normalización map -> agrupación groupByKey (Shuffle)
    -> agregación reduceByKey (Shuffle) -> Acción saveAsTextFile().
    Genera múltiples Stages separados por Shuffle Boundaries en red.
    """
    raw_rdd = driver.parallelize(
        name="raw_transactions_2026",
        num_partitions=num_partitions,
        total_data_size_mb=data_size_mb,
        skew_factor=skew_factor
    )
    cleaned_rdd = raw_rdd.map(name="map(normalize_currency)")
    grouped_rdd = cleaned_rdd.group_by_key(
        name="groupByKey(client_id)",
        num_partitions=num_partitions,
        skew_factor=skew_factor
    )
    aggregated_rdd = grouped_rdd.reduce_by_key(name="reduceByKey(sum_balance)")

    action_name = "saveAsTextFile(/hdfs/warehouse/balances)"
    job_name = "Data Pipeline: Monthly Balance Aggregation"
    return aggregated_rdd, action_name, user_id, job_name

def create_ml_training_job(
    driver: DriverNode,
    user_id: str = "scientist_laura",
    data_size_mb: float = 160.0,
    num_partitions: int = 4
) -> Tuple[RDD, str, str, str]:
    """
    Trabajo tipo Data Scientist (Machine Learning Iterativo):
    Carga de features -> normalización -> .persist() (Caché en RAM de los Workers)
    -> Acción de optimización/conteo de gradiente.
    Muestra la ventaja central de Spark frente a Hadoop: persistencia en memoria RAM.
    """
    raw_rdd = driver.parallelize(
        name="iris_feature_matrix",
        num_partitions=num_partitions,
        total_data_size_mb=data_size_mb
    )
    normalized_rdd = raw_rdd.map(name="map(standard_scaler)")
    cached_rdd = normalized_rdd.persist(storage_level="MEMORY_ONLY")

    action_name = "collect(weights)"
    job_name = "ML Training: KMeans Iris Centroid Calculation"
    return cached_rdd, action_name, user_id, job_name
