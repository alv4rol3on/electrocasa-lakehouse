from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.empleados_activos_sucursal",
    comment="Cantidad de empleados activos por sucursal"
)
def empleados_activos_sucursal():

    df = spark.read.table(
        "electrocasa.silver.empleados_silver"
    )

    df = df.filter(
        F.col("es_actual") == True
    )

    df = df.groupBy(
        "sucursal_id"
    ).agg(
        F.countDistinct("dni").alias(
            "empleados_activos"
        )
    )

    return df