# Simulador Educativo de Apache Spark ⚡

Simulador interactivo y desacoplado de un clúster de **Apache Spark**, diseñado para la asignatura **Procesamiento de Datos a Gran Escala** de la Pontificia Universidad Javeriana.

Construido utilizando metodologías de desarrollo asistidas por Inteligencia Artificial y aplicando ingeniería de software modular, el simulador modela con rigor los principios teóricos y arquitectónicos presentados en las clases del profesor **John Corredor, PhD**.

---

## 🏛️ Conceptos Teóricos y Arquitectónicos Modelados

El simulador implementa de forma explícita los fundamentos de Big Data cubiertos en el curso:

1. **Arquitectura Maestro-Esclavo**:
   - **Driver (Master):** Mantiene la `SparkSession`, cataloga los RDDs, descompone las Acciones en grafos de ejecución física (*DAGScheduler*) y monitorea la salud del clúster.
   - **Workers (Executors):** Nodos con capacidad finita de cómputo (Cores de CPU) y gestión particionada de memoria RAM dividida en **Execution Memory** (para cómputo y shuffle) y **Storage Memory** (para caché de RDDs persistidos).

2. **Abstracción RDD (Resilient Distributed Dataset)**:
   - **Inmutabilidad:** Cada transformación genera una nueva instancia de RDD sin mutar el origen.
   - **Particionado (Sharding Horizontal):** Los macro-datos se fragmentan en particiones atómicas distribuidas entre los núcleos de los Workers.
   - **Lazy Evaluation (Evaluación Perezosa):** Las transformaciones (`map`, `filter`, `groupByKey`, `reduceByKey`) no se computan de inmediato; únicamente construyen el grafo de dependencias lógicas. La computación física arranca estrictamente al invocar una **Acción** (`count`, `saveAsTextFile`, `collect`).
   - **Dependencias Narrow vs Wide:**
     - *Narrow (map, filter):* Cada partición hija depende de exactamente una partición padre. Se ejecutan dentro de la misma etapa física (*Pipelining*).
     - *Wide (groupByKey, reduceByKey):* Requieren intercambiar datos entre múltiples nodos por la red (*Shuffle*), definiendo fronteras estrictas entre **Stages** (*ShuffleMapStage* y *ResultStage*).

3. **Mecanismo de Resiliencia y Tolerancia a Fallos (Linaje / Lineage DAG)**:
   - A diferencia de sistemas basados en disco como Hadoop HDFS (que replican $R=3$ copias físicas), Spark logra resiliencia en memoria mediante su **grafo de linaje**.
   - Si un nodo Worker se cae en plena ejecución, el Driver detecta la pérdida, consulta el árbol de linaje del RDD y **recalcula únicamente las particiones perdidas en los nodos supervivientes**, sin abortar ni reiniciar el trabajo completo.

4. **Planificación FIFO (First-In, First-Out)**:
   - Los trabajos de los diferentes usuarios se encolan y atienden en estricto orden de llegada, permitiendo analizar la contención de núcleos de CPU y tiempos de espera en cola (*Wait Time*).

5. **Perfiles de Usuario (Workload Profiles)**:
   - **Business Analyst:** Consultas interactivas rápidas (`filter + count`), dependencias Narrow y baja latencia.
   - **Data Engineer:** Procesamiento por lotes (Batch ETL) con transformaciones Wide, intercambio pesado de Shuffle y persistencia final en almacenamiento.
   - **Data Scientist:** Cómputo iterativo de Machine Learning haciendo uso explícito de `.persist()` en memoria RAM para acelerar re-ejecuciones.

---

## 📂 Estructura Modular del Proyecto

El código está estrictamente dividido en módulos especializados con responsabilidades desacopladas:

```text
simulador_spark/
├── config.py                 # Constantes de estado (DependencyType, TaskStatus) y configuración del clúster
│
├── models/                   # Modelos de datos del dominio Spark
│   ├── partition.py          # Partición atómica de RDD (tamaño MB, estado, caché, recálculos)
│   ├── rdd.py                # Clase RDD, transformaciones Narrow/Wide, persist() y árbol de linaje (DFS)
│   ├── worker.py             # WorkerNode: gestión de cores, Execution vs Storage RAM y fallos físicos
│   ├── driver.py             # DriverNode: SparkSession/Coordinador, catálogo de RDDs y creación de Jobs
│   └── job.py                # Descomposición física: Task, Stage (ShuffleMap vs Result) y Job
│
├── engine/                   # Motores algorítmicos del clúster
│   ├── dag_scheduler.py      # Traductor de DAG lógico a Stages físicos y fronteras de Shuffle
│   ├── task_scheduler.py     # Planificador de tareas bajo política FIFO y localidad de datos
│   ├── fault_manager.py      # Inyección de fallos de nodos y RECÁLCULO POR LINAJE
│   └── simulator.py          # Bucle principal de simulación por ticks discretos
│
├── workload/                 # Generación de tráfico y perfiles de usuario
│   ├── user_profiles.py      # Generadores de consultas para Analista, Ingeniero y Científico
│   └── workload_generator.py # Gestor de peticiones individuales y ráfagas concurrentes
│
├── metrics/                  # Telemetría y KPIs
│   └── tracker.py            # Series temporales de CPU, RAM, Shuffle MB, Latencias y Fallos
│
├── scenarios/                # Banco de pruebas pedagógicas predefinidas
│   └── test_scenarios.py     # 4 escenarios reproducibles listos para sustentación
│
├── ui/                       # Interfaz gráfica interactiva (Spark Web UI)
│   ├── server.py             # Servidor HTTP y API REST (ThreadingHTTPServer, cero dependencias externas)
│   └── static/               # Frontend interactivo
│       ├── index.html        # Estructura del panel de control
│       ├── styles.css        # Estilos modernos enterprise estilo Spark UI
│       └── app.js            # Lógica reactiva de polling, rendering y Chart.js
│
└── main.py                   # Lanzador principal del sistema (Modo Web UI o Modo Tests)
```

---

## 🚀 Instrucciones de Ejecución

El proyecto utiliza únicamente la biblioteca estándar de Python y librerías básicas ya instaladas, garantizando compatibilidad total sin requerir dependencias complejas.

### 1. Iniciar la Interfaz Web Interactiva (Recomendado para Exposición)

Ejecuta el script principal desde la terminal:

```powershell
python main.py
```

* El servidor arrancará en `http://127.0.0.1:8080`.
* Se abrirá automáticamente tu navegador web por defecto con el panel interactivo.
* Puedes especificar un puerto alternativo si lo deseas:
  ```powershell
  python main.py --port 9000
  ```

---

### 2. Ejecutar las Pruebas Automatizadas en Consola

Para validar rápidamente los 4 escenarios de prueba sin abrir la interfaz gráfica:

```powershell
python main.py --mode test
```

Salida esperada:
```text
======================================================================
  EJECUTANDO BANCO DE PRUEBAS AUTOMATIZADAS DE APACHE SPARK
======================================================================

-> [Prueba 1] Escenario 1: Ejecucion Normal (Batch & Consultas)...
  [OK] Finalizado en 11 ticks. Jobs completados: 2

-> [Prueba 2] Escenario 2: Caida de Worker y Tolerancia a Fallos por Linaje...
  [OK] Finalizado en 10 ticks. Tareas recalculadas por linaje: 3

-> [Prueba 3] Escenario 3: Data Skew (Sesgo de Datos y Stragglers)...
  [OK] Finalizado en 17 ticks. Jobs completados: 1

-> [Prueba 4] Escenario 4: Concurrencia Multiusuario y Cola FIFO...
  [OK] Finalizado en 17 ticks. Total de jobs atendidos: 3

======================================================================
  TODAS LAS PRUEBAS AUTOMATIZADAS PASARON SATISFACTORIAMENTE (100%)
======================================================================
```

---

## 🧪 Guía para la Sustentación / Demostración en Clase

En la barra superior del Dashboard encontrarás botones de un solo clic para cargar los 4 escenarios de prueba:

### Escenario 1: Ejecución Normal y Balanceada
* **Qué presionar:** Clic en botón `[#1: Ejecución Normal]`, luego clic en `[Iniciar]`.
* **Qué observar:** 
  1. El trabajo se divide en dos etapas: `Stage-1 (ShuffleMap)` y `Stage-2 (Result)`.
  2. Los 16 cores (4 workers $\times$ 4 cores) se activan en paralelo.
  3. Las tareas terminan de forma sincronizada y el gráfico de CPU sube a casi 100% y desciende limpiamente.

### Escenario 2: Caída de Worker y Resiliencia por Linaje (Punto Clave ⭐)
* **Qué presionar:** Clic en botón `[#2: Caída de Worker y Linaje]`, luego clic en `[Iniciar]`.
* **Qué hacer en vivo:** Cuando el progreso vaya en ~30-40%, haz clic en el botón rojo **"Matar Nodo"** de `worker-2`.
* **Qué explicarle al profesor:**
  1. El nodo `worker-2` pasa a estado **FALLIDO** (rojo) y sus núcleos caen a cero.
  2. En la consola de eventos aparece: `WORKER_CRASH` y seguidamente `LINEAGE_RECOMPUTATION`.
  3. En la matriz de tareas verás las celdas en color naranja parpadeante (*RECOMPUTING*).
  4. **Conclusión conceptual:** *"Spark no reinició el trabajo completo. Gracias al DAG de linaje, el Driver supo qué transformaciones crearon esas particiones específicas y las reasignó a worker-1 y worker-3, completando el trabajo exitosamente"*.

### Escenario 3: Sesgo de Datos (Data Skew y Tareas Rezagadas)
* **Qué presionar:** Clic en botón `[#3: Data Skew]`, luego clic en `[Iniciar]`.
* **Qué observar:**
  1. La partición 0 tiene un factor de sesgo de 3.5x.
  2. Todas las tareas de las demás particiones terminan en pocos ticks, pero la tarea #1 continúa en ejecución.
  3. El `Stage-2` no puede comenzar porque el Shuffle depende de que todas las particiones del `Stage-1` terminen.
  4. Muestra visualmente por qué el *Data Skew* es uno de los mayores problemas de rendimiento en Big Data.

### Escenario 4: Concurrencia Multiusuario y Cola FIFO
* **Qué presionar:** Clic en botón `[#4: Concurrencia Multiusuario]`, luego clic en `[Iniciar]`.
* **Qué observar:**
  1. Entran simultáneamente trabajos de María (Analista), Carlos (Ingeniero) y Laura (Científica).
  2. En el panel de **Cola FIFO de Espera**, los trabajos se ordenan en fila.
  3. A medida que el trabajo al frente termina, el siguiente toma de inmediato los núcleos liberados del clúster.
