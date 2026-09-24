from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ================================================================
# TRACKING SILVER
# ================================================================

@dp.materialized_view(
    name="electrocasa.silver.tracking_envios_silver",
    comment="Tracking de envíos limpio y estandarizado"
)
@dp.expect_or_drop(
    "fecha_actualizacion_valida",
    "fecha_actualizacion IS NOT NULL"
)
def tracking_envios_silver():

    df = spark.read.table(
        "electrocasa.bronze.tracking_bronze"
    )

    # ============================================================
    # NORMALIZAR ESTADO DE ENTREGA
    # ============================================================

    df = df.withColumn(
        "estado_entrega",
        F.when(
            F.lower(F.trim(F.col("estado_entrega"))).isin(
                "en_camino",
                "en camino",
                "en_transito"
            ),
            F.lit("en_transito")
        )
        .when(
            F.lower(F.trim(F.col("estado_entrega"))) == "pendiente",
            F.lit("pendiente")
        )
        .when(
            F.lower(F.trim(F.col("estado_entrega"))) == "entregado",
            F.lit("entregado")
        )
        .when(
            F.lower(F.trim(F.col("estado_entrega"))) == "devuelto",
            F.lit("devuelto")
        )
        .otherwise(
            F.lower(F.trim(F.col("estado_entrega")))
        )
    )

    # ============================================================
    # ELIMINAR DUPLICADOS EXACTOS
    # ============================================================

    df = df.dropDuplicates([
        "tracking_id",
        "pedido_id",
        "courier",
        "estado_entrega",
        "sucursal_origen",
        "fecha_actualizacion"
    ])

    return df


# ================================================================
# TRACKING QUARANTINE
# ================================================================

@dp.materialized_view(
    name="electrocasa.silver.tracking_quarantine",
    comment="Registros de Tracking rechazados por reglas de calidad"
)
def tracking_quarantine():

    df = spark.read.table(
        "electrocasa.bronze.tracking_bronze"
    )

    # ============================================================
    # NORMALIZAR ESTADO DE ENTREGA
    # ============================================================

    df = df.withColumn(
        "estado_entrega",
        F.when(
            F.lower(F.trim(F.col("estado_entrega"))).isin(
                "en_camino",
                "en camino",
                "en_transito"
            ),
            F.lit("en_transito")
        )
        .when(
            F.lower(F.trim(F.col("estado_entrega"))) == "pendiente",
            F.lit("pendiente")
        )
        .when(
            F.lower(F.trim(F.col("estado_entrega"))) == "entregado",
            F.lit("entregado")
        )
        .when(
            F.lower(F.trim(F.col("estado_entrega"))) == "devuelto",
            F.lit("devuelto")
        )
        .otherwise(
            F.lower(F.trim(F.col("estado_entrega")))
        )
    )

    # ============================================================
    # ELIMINAR DUPLICADOS EXACTOS
    # ============================================================

    df = df.dropDuplicates([
        "tracking_id",
        "pedido_id",
        "courier",
        "estado_entrega",
        "sucursal_origen",
        "fecha_actualizacion"
    ])

    # ============================================================
    # FILTRAR REGISTROS RECHAZADOS
    # ============================================================

    df = df.filter(
        F.col("fecha_actualizacion").isNull()
    )

    # ============================================================
    # MOTIVO DEL RECHAZO
    # ============================================================

    return (
        df
        .withColumn(
            "motivo_rechazo",
            F.lit("fecha_actualizacion_nula")
        )
        .withColumn(
            "fecha_rechazo",
            F.current_timestamp()
        )
    )