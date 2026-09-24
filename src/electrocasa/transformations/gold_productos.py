from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="electrocasa.gold.productos_ventas_devoluciones",
    comment="Ranking de productos por ventas y devoluciones"
)
def productos_ventas_devoluciones():

    ventas = (
        spark.read.table("electrocasa.silver.ventas_silver")
        .groupBy("producto_id")
        .agg(
            F.countDistinct("venta_id").alias("cantidad_ventas"),
            F.sum("cantidad").alias("unidades_vendidas"),
            F.sum("monto_total").alias("monto_vendido")
        )
    )

    devoluciones = (
        spark.read.table("electrocasa.silver.devoluciones_silver")
        .groupBy("producto_id")
        .agg(
            F.countDistinct("devolucion_id").alias("cantidad_devoluciones"),
            F.sum("monto_reembolso").alias("monto_reembolsado")
        )
    )

    df = ventas.join(
        devoluciones,
        on="producto_id",
        how="full"
    )

    df = df.fillna({
        "cantidad_ventas": 0,
        "unidades_vendidas": 0,
        "monto_vendido": 0.0,
        "cantidad_devoluciones": 0,
        "monto_reembolsado": 0.0
    })

    # ============================================================
    # RANKING DE PRODUCTOS
    # ============================================================

    ventana_ventas = Window.orderBy(
        F.col("unidades_vendidas").desc()
    )

    ventana_devoluciones = Window.orderBy(
        F.col("cantidad_devoluciones").desc()
    )

    df = df.withColumn(
        "ranking_ventas",
        F.dense_rank().over(ventana_ventas)
    )

    df = df.withColumn(
        "ranking_devoluciones",
        F.dense_rank().over(ventana_devoluciones)
    )

    return df