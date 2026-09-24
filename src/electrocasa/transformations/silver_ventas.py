from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ================================================================
# FUNCION PARA NORMALIZAR METODO DE PAGO
# ================================================================

def normalizar_metodo_pago(df):

    return df.withColumn(
        "metodo_pago",
        F.when(
            F.lower(F.trim(F.col("metodo_pago"))).isin(
                "efectivo",
                "cash"
            ),
            F.lit("efectivo")
        )
        .when(
            F.lower(F.trim(F.col("metodo_pago"))) == "plin",
            F.lit("plin")
        )
        .when(
            F.lower(F.trim(F.col("metodo_pago"))) == "yape",
            F.lit("yape")
        )
        .when(
            F.lower(F.trim(F.col("metodo_pago"))).isin(
                "transferencia",
                "transferencia bancaria"
            ),
            F.lit("transferencia")
        )
        .when(
            F.lower(F.trim(F.col("metodo_pago"))).isin(
                "tc",
                "tarjeta",
                "tarjeta de credito",
                "tarjeta_credito"
            ),
            F.lit("tarjeta")
        )
        .otherwise(
            F.lower(F.trim(F.col("metodo_pago")))
        )
    )


# ================================================================
# VENTAS SILVER
# ================================================================

@dp.materialized_view(
    name="electrocasa.silver.ventas_silver",
    comment="Ventas limpias, estandarizadas y deduplicadas"
)
@dp.expect_or_drop(
    "sucursal_id_valida",
    "sucursal_id IS NOT NULL"
)
@dp.expect_or_drop(
    "monto_total_valido",
    "monto_total IS NOT NULL AND monto_total > 0"
)
def ventas_silver():

    df = spark.read.table(
        "electrocasa.bronze.ventas_bronze"
    )

    # ============================================================
    # NORMALIZAR METODO DE PAGO
    # ============================================================

    df = normalizar_metodo_pago(df)

    # ============================================================
    # ELIMINAR DUPLICADOS POR venta_id
    # ============================================================

    df = df.dropDuplicates(["venta_id"])

    return df


# ================================================================
# VENTAS QUARANTINE
# ================================================================

@dp.materialized_view(
    name="electrocasa.silver.ventas_quarantine",
    comment="Registros de ventas rechazados por reglas de calidad"
)
def ventas_quarantine():

    df = spark.read.table(
        "electrocasa.bronze.ventas_bronze"
    )

    # Normalizar método de pago también en quarantine
    df = normalizar_metodo_pago(df)

    # Mismo criterio de deduplicación utilizado en Silver
    df = df.dropDuplicates(["venta_id"])

    # ============================================================
    # FILTRAR REGISTROS RECHAZADOS
    # ============================================================

    df = df.filter(
        F.col("sucursal_id").isNull()
        | F.col("monto_total").isNull()
        | (F.col("monto_total") <= 0)
    )

    # ============================================================
    # MOTIVO DEL RECHAZO
    # ============================================================

    df = df.withColumn(
        "motivo_rechazo",
        F.when(
            F.col("sucursal_id").isNull(),
            F.lit("sucursal_id_nulo")
        )
        .when(
            F.col("monto_total").isNull(),
            F.lit("monto_total_nulo")
        )
        .when(
            F.col("monto_total") <= 0,
            F.lit("monto_total_menor_igual_cero")
        )
        .otherwise(
            F.lit("otro")
        )
    )

    return df.withColumn(
        "fecha_rechazo",
        F.current_timestamp()
    )