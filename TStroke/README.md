# Clasificación de casos de derrame cerebral con PySpark

**Pontificia Universidad Javeriana**  
Procesamiento de Datos a Gran Escala · Taller de clasificación  
**Autor** Andrés Felipe Díaz Torres · **Profesor** John Corredor

[Ver el cuaderno del taller](./DiazICTUS_1055.ipynb)

## Qué se hizo en el taller

El cuaderno explora registros de pacientes con y sin derrame cerebral y compara cinco clasificadores de Spark MLlib para la variable objetivo `Derrame` (0 = sin derrame, 1 = con derrame). La preparación incluye revisión de los datos, imputación de valores faltantes del BMI por grupo de edad y género, codificación de variables categóricas y construcción del vector de características.

La evaluación usa un mismo conjunto de prueba para los modelos. El cuaderno incluye métricas generales, matrices de confusión y medidas específicas para los casos con derrame, que son la clase minoritaria.

## Resultados de la ejecución guardada

Se ejecutaron los cinco modelos en el clúster del curso. El conjunto de prueba incluyó **1055 registros**, de los cuales **51 correspondían a pacientes con derrame**.

| Modelo | Derrames detectados | Derrames omitidos | Falsas alarmas | Sensibilidad (Derrame = 1) | Precisión (Derrame = 1) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Regresión logística | 0 | 51 | 0 | 0,00 % | — |
| Naive Bayes | 30 | 21 | 173 | 58,82 % | 14,78 % |
| Árbol de decisión | 0 | 51 | 4 | 0,00 % | 0,00 % |
| Random Forest | 0 | 51 | 0 | 0,00 % | — |
| Multilayer Perceptron | 1 | 50 | 7 | 1,96 % | 12,50 % |

El guion indica que el modelo no predijo ningún caso positivo y la precisión para esa clase quedó indefinida.

**Naive Bayes** detectó más derrames en esta partición y también generó más falsas alarmas. La gráfica del cuaderno compara los casos detectados y omitidos por cada modelo.

## Cómo abrir y ejecutar el cuaderno

1. Abre el [archivo `.ipynb`](./DiazICTUS_1055.ipynb) para consultar las gráficas, métricas y conclusiones guardadas.
2. Para volver a ejecutarlo, prepara un entorno con Python, Jupyter, una instalación compatible de Java y Apache Spark, y las bibliotecas `pyspark`, `findspark`, `numpy`, `pandas`, `scikit-learn`, `matplotlib` y `seaborn`.
3. El cuaderno está configurado para el entorno Spark/HDFS del curso. En otro entorno, adapta la ruta de Spark, el comando de HDFS y la celda que carga `stroke_pyspark.csv`. Si tienes una copia local del CSV, puedes leerla directamente con Spark.
4. Ejecuta las celdas en orden. La división de entrenamiento y prueba es aleatoria, así que una ejecución nueva puede producir valores distintos de los de la tabla.
