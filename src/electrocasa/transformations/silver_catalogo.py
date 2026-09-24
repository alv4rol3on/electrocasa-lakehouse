from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ================================================================
# FUNCION PARA NORMALIZAR CATEGORIA
# ================================================================

def normalizar_categoria(df):

    return df.withColumn(
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

    df = spark.read.table(
        "electrocasa.bronze.catalogo_bronze"
    )

    # Normalizar categoría
    df = normalizar_categoria(df)

    # Obtener producto_id que aparecen más de una vez
    duplicados = (
        df
        .groupBy("producto_id")
        .count()
        .filter(F.col("count") > 1)
        .select("producto_id")
    )

    # Eliminar todas las filas pertenecientes a IDs duplicados
    df = (
        df
        .join(
            duplicados,
            on="producto_id",
            how="left_anti"
        )
    )

    return df


# ================================================================
# CATALOGO QUARANTINE
# ================================================================

@dp.materialized_view(
    name="catalogo_quarantine",
    comment="Registros de catálogo rechazados por reglas de calidad"
)
def catalogo_quarantine():

    df = spark.read.table(
        "electrocasa.bronze.catalogo_bronze"
    )

    # Normalizar categoría
    df = normalizar_categoria(df)

    # Obtener IDs duplicados
    duplicados = (
        df
        .groupBy("producto_id")
        .count()
        .filter(F.col("count") > 1)
        .select("producto_id")
    )

    # Marcar registros duplicados
    df = (
        df
        .join(
            duplicados
            .withColumn(
                "_producto_id_duplicado",
                F.lit(True)
            ),
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

    # Registros rechazados
    df = df.filter(
        F.col("_producto_id_duplicado")
        | F.col("precio_lista").isNull()
        | (F.col("precio_lista") <= 0)
    )

    # Motivo del rechazo
    df = df.withColumn(
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

    return (
        df
        .withColumn(
            "fecha_rechazo",
            F.current_timestamp()
        )
        .drop("_producto_id_duplicado")
    )