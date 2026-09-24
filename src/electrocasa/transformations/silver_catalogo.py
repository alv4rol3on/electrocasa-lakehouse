from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ================================================================
# PREPARAR CATALOGO
# ================================================================

@dp.temporary_view(name="catalogo_preparado")
def catalogo_preparado():

    df = spark.read.table("electrocasa.bronze.catalogo_bronze")

    # ============================================================
    # NORMALIZAR CATEGORIA
    # ============================================================

    df = df.withColumn(
        "categoria",
        F.when(
            F.lower(F.trim(F.col("categoria"))).isin(
                "electronica",
                "electrónica"
            ),
            F.lit("electronica")
        )
        .when(
            F.lower(F.trim(F.col("categoria"))).isin(
                "linea blanca",
                "linea_blanca",
                "línea blanca"
            ),
            F.lit("linea_blanca")
        )
        .when(
            F.lower(F.trim(F.col("categoria"))).isin(
                "climatizacion",
                "climatización"
            ),
            F.lit("climatizacion")
        )
        .when(
            F.lower(F.trim(F.col("categoria"))) == "entretenimiento",
            F.lit("entretenimiento")
        )
        .when(
            F.lower(F.trim(F.col("categoria"))) == "cocina",
            F.lit("cocina")
        )
        .otherwise(
            F.lower(F.trim(F.col("categoria")))
        )
    )

    # ============================================================
    # IDENTIFICAR PRODUCTOS DUPLICADOS
    # ============================================================

    duplicados = (
        df
        .groupBy("producto_id")
        .count()
        .filter(F.col("count") > 1)
        .select("producto_id")
        .withColumn("_producto_id_duplicado", F.lit(True))
    )

    df = (
        df
        .join(
            duplicados,
            on="producto_id",
            how="left"
        )
        .withColumn(
            "_producto_id_duplicado",
            F.coalesce(
                F.col("_producto_id_duplicado"),
                F.lit(False)
            )
        )
    )

    return df


# ================================================================
# CATALOGO SILVER
# ================================================================

@dp.materialized_view(
    name="catalogo_silver",
    comment="Catálogo limpio, normalizado y sin productos duplicados"
)
@dp.expect_or_drop(
    "precio_lista_valido",
    "precio_lista IS NOT NULL AND precio_lista > 0"
)
def catalogo_silver():

    return (
        spark.read.table("catalogo_preparado")
        .filter(
            F.col("_producto_id_duplicado") == False
        )
        .drop("_producto_id_duplicado")
    )


# ================================================================
# CATALOGO QUARANTINE
# ================================================================

@dp.materialized_view(
    name="catalogo_quarantine",
    comment="Registros de catálogo rechazados por reglas de calidad"
)
def catalogo_quarantine():

    df = spark.read.table("catalogo_preparado")

    return (
        df
        .filter(
            F.col("_producto_id_duplicado")
            | F.col("precio_lista").isNull()
            | (F.col("precio_lista") <= 0)
        )
        .withColumn(
            "motivo_rechazo",
            F.when(
                F.col("_producto_id_duplicado"),
                F.lit("producto_id_duplicado")
            )
            .when(
                F.col("precio_lista").isNull(),
                F.lit("precio_lista_nulo")
            )
            .when(
                F.col("precio_lista") <= 0,
                F.lit("precio_lista_menor_igual_cero")
            )
            .otherwise(
                F.lit("otro")
            )
        )
        .withColumn(
            "fecha_rechazo",
            F.current_timestamp()
        )
        .drop("_producto_id_duplicado")
    )