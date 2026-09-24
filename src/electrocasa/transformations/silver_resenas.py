from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# NORMALIZAR RESEÑAS
# ============================================================

def preparar_resenas(df):

    # --------------------------------------------------------
    # Calificación
    # --------------------------------------------------------
    df = df.withColumn(
        "calificacion",
        F.col("calificacion").cast("int")
    )

    # --------------------------------------------------------
    # Fecha de reseña
    #
    # La fuente viene como STRING, por lo que la convertimos
    # a DATE para trabajar correctamente en Silver.
    # --------------------------------------------------------
    df = df.withColumn(
        "fecha_resena",
        F.to_date(F.col("fecha_resena"))
    )

    # --------------------------------------------------------
    # IDs
    # --------------------------------------------------------
    df = df.withColumn(
        "cliente_id",
        F.trim(F.col("cliente_id"))
    )

    df = df.withColumn(
        "producto_id",
        F.trim(F.col("producto_id"))
    )

    df = df.withColumn(
        "resena_id",
        F.trim(F.col("resena_id"))
    )

    # --------------------------------------------------------
    # Comentario
    #
    # No eliminamos comentarios nulos porque una reseña puede
    # seguir siendo válida aunque no tenga comentario.
    # --------------------------------------------------------
    df = df.withColumn(
        "comentario",
        F.trim(F.col("comentario"))
    )

    return df


# ============================================================
# SILVER - RESEÑAS
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.resenas_silver",
    comment="Reseñas limpias, estandarizadas y deduplicadas"
)
@dp.expect_or_drop(
    "resena_id_valido",
    "resena_id IS NOT NULL"
)
@dp.expect_or_drop(
    "calificacion_valida",
    "calificacion IS NOT NULL AND calificacion BETWEEN 1 AND 5"
)
@dp.expect_or_drop(
    "fecha_resena_valida",
    "fecha_resena IS NOT NULL"
)
def resenas_silver():

    # --------------------------------------------------------
    # Leer Bronze
    # --------------------------------------------------------

    df = spark.read.table(
        "electrocasa.bronze.resenas_bronze"
    )

    # --------------------------------------------------------
    # Limpieza y estandarización
    # --------------------------------------------------------

    df = preparar_resenas(df)

    # --------------------------------------------------------
    # Eliminar reseñas duplicadas
    #
    # resena_id identifica de manera única la reseña.
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["resena_id"]
    )

    return df


# ============================================================
# QUARANTINE - RESEÑAS
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.resenas_quarantine",
    comment="Reseñas rechazadas por reglas de calidad"
)
def resenas_quarantine():

    # --------------------------------------------------------
    # Leer Bronze
    # --------------------------------------------------------

    df = spark.read.table(
        "electrocasa.bronze.resenas_bronze"
    )

    # --------------------------------------------------------
    # Limpieza y estandarización
    # --------------------------------------------------------

    df = preparar_resenas(df)

    # --------------------------------------------------------
    # Eliminar duplicados antes de identificar los registros
    # rechazados.
    # --------------------------------------------------------

    df = df.dropDuplicates(
        ["resena_id"]
    )

    # --------------------------------------------------------
    # Identificar registros rechazados
    #
    # 1. resena_id nulo
    # 2. calificacion nula
    # 3. calificacion fuera de rango 1-5
    # 4. fecha_resena nula/inválida
    # --------------------------------------------------------

    df = df.filter(
        F.col("resena_id").isNull()
        | F.col("calificacion").isNull()
        | (F.col("calificacion") < 1)
        | (F.col("calificacion") > 5)
        | F.col("fecha_resena").isNull()
    )

    # --------------------------------------------------------
    # Motivo del rechazo
    # --------------------------------------------------------

    df = df.withColumn(
        "motivo_rechazo",
        F.when(
            F.col("resena_id").isNull(),
            F.lit("resena_id_nulo")
        )
        .when(
            F.col("calificacion").isNull(),
            F.lit("calificacion_nula")
        )
        .when(
            (F.col("calificacion") < 1)
            | (F.col("calificacion") > 5),
            F.lit("calificacion_fuera_rango")
        )
        .when(
            F.col("fecha_resena").isNull(),
            F.lit("fecha_resena_nula")
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