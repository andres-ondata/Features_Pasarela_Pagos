# Proyecto: Pipeline de Datos para Features de ML - Plataforma de Pagos

## 1. Descripción del Proyecto

Este proyecto implementa un pipeline de ingeniería de datos utilizando Python y PySpark para procesar, transformar y enriquecer datos de interacción de usuarios dentro de la aplicación de **una billetera digital**. El objetivo final es construir un dataset de *features* (características) listo para ser consumido por un modelo de Machine Learning.

El modelo busca predecir y optimizar el orden de un conjunto de **Propuestas de Valor** (Value Props) mostradas en el carrusel "Descubrí Más" de la aplicación.

## 2. Fuentes de Datos

El pipeline ingiere datos de tres fuentes distintas que representan un mes de historial de interacciones:

* **`prints.json`**: Un historial de todas las *value props* que fueron mostradas (impresiones) a cada usuario. Contiene el día, ID de usuario y la *value prop* mostrada junto con su posición.
* **`taps.json`**: Un historial de las *value props* que recibieron un clic (tap) por parte de los usuarios. Su estructura es similar a la de los prints.
* **`pays.csv`**: Un historial de los pagos realizados por los usuarios, asociados a una *value prop* específica. Contiene la fecha, el monto total, el ID de usuario y la *value prop*.

## 3. Objetivo y Resultado Esperado

El resultado del pipeline es un único dataset consolidado que servirá como entrada para el modelo de ML. La estructura de este dataset se define de la siguiente manera:

* **Unidad de análisis**: Cada fila corresponde a un **print (impresión) de la última semana** de datos disponibles.
* **Features por cada print**:
    * Un campo booleano (`was_clicked`) que indique si el print recibió un clic.
    * La cantidad de veces que el usuario vio (`prints_3w_before`) cada `value_prop` en las 3 semanas previas a la fecha del print.
    * La cantidad de veces que el usuario hizo clic (`taps_3w_before`) en cada `value_prop` en las 3 semanas previas.
    * La cantidad de pagos (`pays_3w_before`) que el usuario realizó para cada `value_prop` en las 3 semanas previas.
    * El importe acumulado (`amount_3w_before`) que el usuario gastó en cada `value_prop` en las 3 semanas previas.

## 4. Arquitectura y Decisiones Técnicas

* **Framework**: **PySpark**. Elegido por su capacidad para procesar grandes volúmenes de datos de manera distribuida y escalable, lo cual es esencial para un entorno de producción.
* **Plataforma**: El código está diseñado para ser ejecutado en un entorno como **Databricks**, que provee un clúster de Spark gestionado y optimizado.
* **Almacenamiento**: El dataset final se almacena en formato **Delta Lake**. Este formato garantiza transacciones ACID, fiabilidad de los datos y un rendimiento superior para cargas de trabajo analíticas.
* **Modularidad**: El código se presenta en dos formatos:
    1.  `feature_pipeline.py`: Un script modular para ejecuciones automatizadas y jobs programados.
    2.  `Feature_Pipeline.ipynb`: Un notebook interactivo para desarrollo, análisis exploratorio y depuración.

## 5. Estructura del Proyecto

├── data/
│   ├── prints.json
│   ├── taps.json
│   └── pays.csv
├── src/
│   ├── feature_pipeline.py
│   └── Feature_Pipeline.ipynb
├── output_feature_dataset/
│   └── (Directorio con archivos Delta/Parquet generado por el pipeline)
└── README.md

## 6. Requisitos e Instalación

Para ejecutar este proyecto, necesitas un entorno con:
* Java 8 o superior (requerido por Spark).
* Apache Spark 3.x.
* Python 3.8 o superior.

* La librería `pyspark`.
