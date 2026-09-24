from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.productos_ventas_devoluciones",
    comment="Resumen de ventas y devoluciones por producto"
)
def productos_ventas_devoluciones():

    # ========================================================
    # VENTAS
    # ========================================================

    ventas = (
        spark.read.table(
            "electrocasa.silver.ventas_silver"
        )
        .groupBy("producto_id")
        .agg(
            F.countDistinct("venta_id").alias(
                "cantidad_ventas"
            ),
            F.sum("cantidad").alias(
                "unidades_vendidas"
            ),
            F.sum("monto_total").alias(
                "monto_vendido"
            )
        )
    )

    # ========================================================
    # DEVOLUCIONES
    # ========================================================

    devoluciones = (
        spark.read.table(
            "electrocasa.silver.devoluciones_silver"
        )
        .groupBy("producto_id")
        .agg(
            F.countDistinct("devolucion_id").alias(
                "cantidad_devoluciones"
            ),
            F.sum("monto_reembolso").alias(
                "monto_reembolsado"
            )
        )
    )

    # ========================================================
    # UNIR VENTAS + DEVOLUCIONES
    # ========================================================

    df = ventas.join(
        devoluciones,
        on="producto_id",
        how="full"
    )

    # ========================================================
    # RELLENAR PRODUCTOS QUE SOLO APARECEN EN UNA FUENTE
    # ========================================================

    df = df.fillna({
        "cantidad_ventas": 0,
        "unidades_vendidas": 0,
        "monto_vendido": 0.0,
        "cantidad_devoluciones": 0,
        "monto_reembolsado": 0.0
    })

    return df