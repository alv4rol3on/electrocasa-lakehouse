from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
        .otherwise(F.lower(F.trim(F.col("categoria"))))
    )

    # ============================================================
    # IDENTIFICAR PRODUCTOS DUPLICADOS
    # ============================================================

    window_producto = Window.partitionBy("producto_id")

    df = df.withColumn(
        "_cantidad_producto_id",
        F.count("*").over(window_producto)
    )

    df = df.withColumn(
        "_producto_id_duplicado",
        F.col("_cantidad_producto_id") > 1
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
@dp.expect_or_drop(
    "producto_id_unico",
    "_producto_id_duplicado = false"
)
def catalogo_silver():

    return (
        spark.read.table("catalogo_preparado")
        .drop("_cantidad_producto_id", "_producto_id_duplicado")
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
            (F.col("precio_lista").isNull())
            | (F.col("precio_lista") <= 0)
            | F.col("_producto_id_duplicado")
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
            .otherwise(F.lit("otro"))
        )
        .withColumn(
            "fecha_rechazo",
            F.current_timestamp()
        )
        .drop("_cantidad_producto_id", "_producto_id_duplicado")
    )