# Classifying annual income with PySpark

**Pontificia Universidad Javeriana** · Procesamiento de Datos a Gran Escala  
**Author** Andrés Felipe Díaz Torres · **Instructor** John Corredor

[Open the executed notebook](./DiazSalario_1055.ipynb)

## Objective

Can five Spark MLlib classifiers identify people whose annual income exceeded USD 50,000 in the 1994 US census data? The notebook compares them on the same official test set, using F1 to assess the balance between precision and recall for the higher-income group.

## Data and preparation

The original files are available from the [UCI Census Income (Adult) dataset](https://archive.ics.uci.edu/dataset/20/census%2Bincome).

| File | Purpose | Records |
| --- | --- | ---: |
| `adult.data` | Training | 32,561 |
| `adult.test` | Testing | 16,281 |

The notebook keeps the original data and each preparation step in separate Spark DataFrames. Missing entries for occupation, work class and country of origin are recorded as *Desconocido* (unknown). These records still have a known income category. The analysis explores age, income by recorded gender, weekly working hours by income group, and class proportions in both partitions. Category encoders, scaling parameters and bin boundaries are fitted on the training set and applied to the test set.

## Results

Of the 16,281 test records, 3,846 had income above USD 50,000. Precision, recall and F1 below refer to that class. The notebook includes a confusion matrix for each model and a chart comparing the five.

| Model | Accuracy | Precision | Recall | F1 | PR AUC |
| --- | ---: | ---: | ---: | ---: | ---: |
| Logistic regression | 85.24% | 72.99% | 59.57% | 0.6560 | 0.7608 |
| Naive Bayes | 79.66% | 54.76% | 80.06% | 0.6503 | 0.7001 |
| Decision tree | 84.18% | 77.45% | 46.62% | 0.5820 | 0.7363 |
| Random forest | 84.74% | 80.02% | 47.17% | 0.5935 | 0.7532 |
| Multilayer perceptron | 85.38% | 71.97% | 62.43% | 0.6686 | 0.7658 |

The **multilayer perceptron** achieved the highest F1 and correctly identified 2,401 higher-income records. **Naive Bayes** detected the most (3,079) and produced 2,544 false positives. **Random forest** had the highest precision and the fewest false positives (453).

## Running the notebook

1. Open the notebook in Jupyter on the course cluster. The saved run used Spark 3.5.5, Hadoop HDFS, PySpark, `findspark`, `pandas` and `matplotlib`.
2. If the input files are missing from HDFS, download them from UCI and place them next to the notebook or in a sibling directory named `census+income`.
3. Set `RUTA_HDFS` to a writable HDFS directory. The notebook creates the directory, copies any missing files and reads both partitions from HDFS.
4. Run the cells in order. On another cluster, adjust the Spark installation path, Hadoop executable, HDFS address and Spark UI port for that environment.
