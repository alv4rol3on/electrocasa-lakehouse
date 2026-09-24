from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# PREPARAR DEVOLUCIONES
# ============================================================

def preparar_devoluciones(df):

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------

    df = df.withColumn(
        "devolucion_id",
        F.trim(F.col("devolucion_id"))
    )

    df = df.withColumn(
        "pedido_id",
        F.trim(F.col("pedido_id"))
    )

    df = df.withColumn(
        "sucursal_id",
        F.trim(F.col("sucursal_id"))
    )

    df = df.withColumn(
        "producto_id",
        F.trim(F.col("producto_id"))
    )

    # --------------------------------------------------------
    # Motivo
    # --------------------------------------------------------

    df = df.withColumn(
        "motivo",
        F.trim(F.col("motivo"))
    )

    # --------------------------------------------------------
    # Monto del reembolso
    # --------------------------------------------------------

    df = df.withColumn(
        "monto_reembolso",
        F.col("monto_reembolso").cast("double")
    )

    # --------------------------------------------------------
    # Fecha de devolución
    # --------------------------------------------------------

    df = df.withColumn(
        "fecha_devolucion",
        F.to_date(F.col("fecha_devolucion"))
    )

    return df


# ============================================================
# SILVER - DEVOLUCIONES
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.devoluciones_silver",
    comment="Devoluciones limpias, estandarizadas y deduplicadas"
)
@dp.expect_or_drop(
    "devolucion_id_valido",
    "devolucion_id IS NOT NULL"
)
@dp.expect_or_drop(
    "pedido_id_valido",
    "pedido_id IS NOT NULL"
)
@dp.expect_or_drop(
    "motivo_valido",
    "motivo IS NOT NULL"
)
@dp.expect_or_drop(
    "monto_reembolso_valido",
    "monto_reembolso IS NOT NULL AND monto_reembolso > 0"
)
@dp.expect_or_drop(
    "fecha_devolucion_valida",
    "fecha_devolucion IS NOT NULL"
)
def devoluciones_silver():

    # --------------------------------------------------------
    # Leer Bronze
    # --------------------------------------------------------

    df = spark.read.table(
        "electrocasa.bronze.devoluciones_bronze"
    )

    # --------------------------------------------------------
    # Limpieza y estandarización
    # --------------------------------------------------------

    df = preparar_devoluciones(df)

    # --------------------------------------------------------
    # Eliminar duplicados
    #
    # devolucion_id identifica de manera única la devolución.
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["devolucion_id"]
    )

    return df


# ============================================================
# QUARANTINE - DEVOLUCIONES
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.devoluciones_quarantine",
    comment="Devoluciones rechazadas por reglas de calidad"
)
def devoluciones_quarantine():

    # --------------------------------------------------------
    # Leer Bronze
    # --------------------------------------------------------

    df = spark.read.table(
        "electrocasa.bronze.devoluciones_bronze"
    )

    # --------------------------------------------------------
    # Limpieza y estandarización
    # --------------------------------------------------------

    df = preparar_devoluciones(df)

    # --------------------------------------------------------
    # Eliminar duplicados antes de identificar los registros
    # rechazados.
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["devolucion_id"]
    )

    # --------------------------------------------------------
    # Identificar registros rechazados
    # --------------------------------------------------------

    df = df.filter(
        F.col("devolucion_id").isNull()
        | F.col("pedido_id").isNull()
        | F.col("motivo").isNull()
        | F.col("monto_reembolso").isNull()
        | (F.col("monto_reembolso") <= 0)
        | F.col("fecha_devolucion").isNull()
    )

    # --------------------------------------------------------
    # Motivo del rechazo
    # --------------------------------------------------------

    df = df.withColumn(
        "motivo_rechazo",
        F.when(
            F.col("devolucion_id").isNull(),
            F.lit("devolucion_id_nulo")
        )
        .when(
            F.col("pedido_id").isNull(),
            F.lit("pedido_id_nulo")
        )
        .when(
            F.col("motivo").isNull(),
            F.lit("motivo_nulo")
        )
        .when(
            F.col("monto_reembolso").isNull(),
            F.lit("monto_reembolso_nulo")
        )
        .when(
            F.col("monto_reembolso") <= 0,
            F.lit("monto_reembolso_menor_igual_cero")
        )
        .when(
            F.col("fecha_devolucion").isNull(),
            F.lit("fecha_devolucion_nula")
        )
        .otherwise(
            F.lit("otro")
        )
    )

    # --------------------------------------------------------
    # Fecha de rechazo
    # --------------------------------------------------------

    return df.withColumn(
        "fecha_rechazo",
        F.current_timestamp()
    )