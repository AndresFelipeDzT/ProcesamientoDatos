# Clasificación de casos de derrame cerebral con PySpark

**Pontificia Universidad Javeriana**  
Procesamiento de Datos a Gran Escala · Taller de clasificación  
**Autor** Andrés Felipe Díaz Torres · **Profesor** John Corredor

[Ver el cuaderno del taller](./DiazICTUS_1055.ipynb)

## Qué se hizo en el taller

El cuaderno explora registros de pacientes con y sin derrame cerebral y compara cuatro clasificadores de Spark MLlib para la variable objetivo `Derrame` (0 = sin derrame, 1 = con derrame). La preparación incluye revisión de los datos, imputación de valores faltantes del BMI por grupo de edad y género, codificación de variables categóricas y construcción del vector de características.

La evaluación usa un mismo conjunto de prueba para los cuatro modelos. El cuaderno incluye métricas generales, matrices de confusión y medidas específicas para los casos con derrame, que son la clase minoritaria.

## Resultados de la ejecución guardada

El conjunto de prueba tuvo **962 registros**, de los cuales **47 correspondían a pacientes con derrame**.

| Modelo | Derrames detectados | Derrames omitidos | Falsas alarmas | Sensibilidad (Derrame = 1) | Precisión (Derrame = 1) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Regresión logística | 0 | 47 | 0 | 0,00 % | — |
| Naive Bayes | 27 | 20 | 183 | 57,45 % | 12,86 % |
| Árbol de decisión | 0 | 47 | 1 | 0,00 % | 0,00 % |
| Random Forest | 0 | 47 | 0 | 0,00 % | — |

El guion indica que el modelo no clasificó ningún caso como positivo y, por tanto, la precisión para esa clase quedó indefinida.

En esta ejecución, **Naive Bayes** fue el único que detectó casos de derrame. A cambio, generó 183 falsas alarmas. La comparación muestra por qué, con una clase minoritaria, conviene mirar las matrices de confusión además de la exactitud general. La preferencia por Naive Bayes corresponde a esta partición y a la prioridad de detectar casos positivos.

## Cómo abrir y ejecutar el cuaderno

1. Abre el [archivo `.ipynb`](./DiazICTUS_1055.ipynb) para consultar las gráficas, métricas y conclusiones guardadas.
2. Para volver a ejecutarlo, prepara un entorno con Python, Jupyter, una instalación compatible de Java y Apache Spark, y las bibliotecas `pyspark`, `findspark`, `numpy`, `pandas`, `scikit-learn`, `matplotlib` y `seaborn`.
3. El cuaderno está configurado para el entorno Spark/HDFS del curso. En otro entorno, adapta la ruta de Spark, el comando de HDFS y la celda que carga `stroke_pyspark.csv`. Si tienes una copia local del CSV, puedes leerla directamente con Spark.
4. Ejecuta las celdas en orden. La división de entrenamiento y prueba es aleatoria, así que una ejecución nueva puede producir valores distintos de los de la tabla.
