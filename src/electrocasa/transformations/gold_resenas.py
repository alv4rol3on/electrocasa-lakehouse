from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.resenas_negativas_categoria",
    comment="Tasa de reseñas negativas por categoría de producto"
)
def resenas_negativas_categoria():

    # ========================================================
    # RESEÑAS
    # ========================================================

    resenas = spark.read.table(
        "electrocasa.silver.resenas_silver"
    ).select(
        "resena_id",
        "producto_id",
        "calificacion"
    )

    # ========================================================
    # CATÁLOGO
    # ========================================================

    catalogo = spark.read.table(
        "electrocasa.silver.catalogo_silver"
    ).select(
        "producto_id",
        "categoria"
    )

    # ========================================================
    # RELACIONAR RESEÑAS CON CATEGORÍA
    # ========================================================

    df = resenas.join(
        catalogo,
        on="producto_id",
        how="left"
    )

    # ========================================================
    # AGREGAR POR CATEGORÍA
    # ========================================================

    df = df.groupBy(
        "categoria"
    ).agg(
        F.countDistinct("resena_id").alias(
            "total_resenas"
        ),
        F.sum(
            F.when(
                F.col("calificacion") <= 2,
                1
            ).otherwise(0)
        ).alias(
            "resenas_negativas"
        )
    )

    # ========================================================
    # CALCULAR TASA
    # ========================================================

    df = df.withColumn(
        "tasa_resenas_negativas",
        F.round(
            F.col("resenas_negativas")
            / F.col("total_resenas")
            * 100,
            2
        )
    )

    return df