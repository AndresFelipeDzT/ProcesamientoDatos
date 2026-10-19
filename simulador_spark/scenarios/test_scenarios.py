"""
Módulo de Casos de Prueba y Escenarios Demostrativos.
Permite ejecutar y validar escenarios pedagógicos que demuestran los conceptos
centrales evaluados por el profesor John Corredor en la materia.
"""

from typing import Dict, Any
from config import ClusterConfig
from engine.simulator import SparkSimulator
from workload.workload_generator import WorkloadGenerator

class ScenarioRunner:
    """
    Ejecutor de escenarios de prueba controlados.
    """

    @staticmethod
    def run_scenario_1_normal_execution(max_ticks: int = 150) -> Dict[str, Any]:
        """
        ESCENARIO 1: Ejecución Normal (Batch + Interactiva).
        Valida:
        - Lazy Evaluation (el job solo arranca tras la Acción).
        - Descomposición en Stages con Shuffle Map y Result Stage.
        - Asignación balanceada de tareas a cores en FIFO.
        """
        sim = SparkSimulator(ClusterConfig(NUM_WORKERS=4, CORES_PER_WORKER=4))
        generator = WorkloadGenerator(sim)

        # Enviar consulta interactiva y pipeline ETL
        generator.submit_analyst_query("analyst_lucia", data_size_mb=60.0)
        generator.submit_etl_pipeline("engineer_marco", data_size_mb=200.0, num_partitions=8)

        # Ejecutar hasta que ambos trabajos terminen
        while sim.current_tick < max_ticks and sim.task_scheduler.get_active_job() is not None:
            sim.step()

        state = sim.get_cluster_state()
        state["scenario_name"] = "Escenario 1: Ejecución Normal y Balanceada"
        state["description"] = (
            "Demuestra el funcionamiento estándar de Spark: traducción de RDDs en Stages, "
            "ejecución en paralelo en los 4 nodos y terminación exitosa de los trabajos."
        )
        return state

    @staticmethod
    def run_scenario_2_fault_tolerance(max_ticks: int = 200) -> Dict[str, Any]:
        """
        ESCENARIO 2: Caída de Worker y Resiliencia por Grafo de Linaje.
        Valida:
        - Inyección de falla en caliente en worker-2 a mitad de la ejecución.
        - Pérdida de particiones en RAM del nodo caído.
        - Detección por parte del Driver y recálculo selectivo mediante RDD Lineage DAG.
        - Finalización exitosa del trabajo a pesar del fallo.
        """
        sim = SparkSimulator(ClusterConfig(NUM_WORKERS=4, CORES_PER_WORKER=4))
        generator = WorkloadGenerator(sim)

        job = generator.submit_etl_pipeline("engineer_diego", data_size_mb=300.0, num_partitions=12)

        killed = False
        while sim.current_tick < max_ticks and sim.task_scheduler.get_active_job() is not None:
            sim.step()

            # Matar worker-2 al tick 3 mientras las tareas están en plena ejecución
            if not killed and sim.current_tick >= 3:
                sim.kill_worker("worker-2")
                killed = True

        state = sim.get_cluster_state()
        state["scenario_name"] = "Escenario 2: Tolerancia a Fallos por Linaje (Resilience)"
        state["description"] = (
            "Demuestra la tolerancia a fallos única de Spark: cuando worker-2 colapsó, "
            "el Driver no reinició el Job desde cero; en su lugar, consultó el grafo de linaje del RDD "
            "y recalculó las particiones perdidas en los nodos supervivientes."
        )
        return state

    @staticmethod
    def run_scenario_3_data_skew(max_ticks: int = 180) -> Dict[str, Any]:
        """
        ESCENARIO 3: Sesgo de Datos (Data Skew) y Tareas Rezagadas (Stragglers).
        Valida:
        - Impacto de un mal particionamiento donde una partición concentra el triple de datos.
        - Cuello de botella: los cores quedan ociosos esperando que termine la tarea rezagada del Stage.
        """
        sim = SparkSimulator(ClusterConfig(NUM_WORKERS=4, CORES_PER_WORKER=4))
        generator = WorkloadGenerator(sim)

        # Enviar pipeline con sesgo extremo (skew_factor = 3.5)
        generator.submit_etl_pipeline(
            user_id="engineer_skewed",
            data_size_mb=300.0,
            num_partitions=8,
            skew_factor=3.5
        )

        while sim.current_tick < max_ticks and sim.task_scheduler.get_active_job() is not None:
            sim.step()

        state = sim.get_cluster_state()
        state["scenario_name"] = "Escenario 3: Data Skew y Tareas Rezagadas (Stragglers)"
        state["description"] = (
            "Demuestra el problema de sesgo de particionado en Big Data: la partición 0 "
            "demoró mucho más tiempo en completarse, dejando a los demás cores esperando el Shuffle."
        )
        return state

    @staticmethod
    def run_scenario_4_multiuser_concurrency(max_ticks: int = 250) -> Dict[str, Any]:
        """
        ESCENARIO 4: Concurrencia Multiusuario y Cola FIFO.
        Valida:
        - Múltiples usuarios enviando trabajos en ráfaga simultánea.
        - Encolamiento ordenado según el planificador FIFO de Spark.
        - Medición de latencias y tiempos de espera en cola.
        """
        sim = SparkSimulator(ClusterConfig(NUM_WORKERS=4, CORES_PER_WORKER=4))
        generator = WorkloadGenerator(sim)

        generator.submit_burst_workload()

        while sim.current_tick < max_ticks and sim.task_scheduler.get_active_job() is not None:
            sim.step()

        state = sim.get_cluster_state()
        state["scenario_name"] = "Escenario 4: Concurrencia Multiusuario y Cola FIFO"
        state["description"] = (
            "Demuestra la atención de múltiples usuarios (Analista, Ingeniero y Científico de Datos) "
            "respetando el orden FIFO y la utilización masiva de los núcleos del clúster."
        )
        return state
