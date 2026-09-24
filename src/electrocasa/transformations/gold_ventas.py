from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.ventas_sucursal_mes",
    comment="Ventas totales y ticket promedio por sucursal y mes"
)
def ventas_sucursal_mes():

    df = spark.read.table(
        "electrocasa.silver.ventas_silver"
    )

    # La fecha puede venir en dos formatos:
    # yyyy-MM-dd
    # dd/MM/yyyy
    df = df.withColumn(
        "fecha_venta_date",
        F.when(
            F.col("fecha_venta").rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date(F.col("fecha_venta"), "yyyy-MM-dd")
        )
        .when(
            F.col("fecha_venta").rlike(r"^\d{2}/\d{2}/\d{4}$"),
            F.to_date(F.col("fecha_venta"), "dd/MM/yyyy")
        )
    )

    df = df.groupBy(
        "sucursal_id",
        F.year("fecha_venta_date").alias("anio"),
        F.month("fecha_venta_date").alias("mes")
    ).agg(
        F.sum("monto_total").alias("ventas_totales"),
        F.countDistinct("venta_id").alias("cantidad_ventas")
    )

    df = df.withColumn(
        "ticket_promedio",
        F.round(
            F.col("ventas_totales") / F.col("cantidad_ventas"),
            2
        )
    )

    return df