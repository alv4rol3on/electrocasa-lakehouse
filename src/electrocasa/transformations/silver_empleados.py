from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ================================================================
# FUNCION PARA NORMALIZAR TIPO DE EVENTO
# ================================================================

def normalizar_tipo_evento(df):

    return df.withColumn(
        "tipo_evento",
        F.when(
            F.lower(F.trim(F.col("tipo_evento"))) == "alta",
            F.lit("alta")
        )
        .when(
            F.lower(F.trim(F.col("tipo_evento"))).isin(
                "transferencia",
                "traslado"
            ),
            F.lit("transferencia")
        )
        .when(
            F.lower(F.trim(F.col("tipo_evento"))).isin(
                "cambio_salario",
                "cambio salario",
                "cambio_salarial"
            ),
            F.lit("cambio_salario")
        )
        .when(
            F.lower(F.trim(F.col("tipo_evento"))).isin(
                "baja",
                "cese",
                "cesado"
            ),
            F.lit("baja")
        )
        .otherwise(
            F.lower(F.trim(F.col("tipo_evento")))
        )
    )


# ================================================================
# EMPLEADOS SILVER - HISTORIZADO
# ================================================================

@dp.materialized_view(
    name="electrocasa.silver.empleados_silver",
    comment="Historial de empleados con historización de altas, transferencias, cambios salariales y bajas"
)
@dp.expect_or_drop(
    "dni_valido",
    "dni IS NOT NULL"
)
def empleados_silver():

    df = spark.read.table(
        "electrocasa.bronze.empleados_bronze"
    )

    # ============================================================
    # ESTANDARIZACION
    # ============================================================

    df = normalizar_tipo_evento(df)

    # Mantener DNI como texto
    df = df.withColumn(
        "dni",
        F.trim(F.col("dni").cast("string"))
    )

    # Estandarizar fecha del evento
    df = df.withColumn(
        "fecha_evento",
        F.to_date(F.col("fecha_evento"))
    )

    # Estandarizar salario
    df = df.withColumn(
        "salario",
        F.col("salario").cast("double")
    )

    # ============================================================
    # HISTORIZACION
    # ============================================================

    window_empleado = (
        Window
        .partitionBy("dni")
        .orderBy(
            F.col("fecha_evento").asc(),
            F.col("tipo_evento").asc()
        )
    )

    df = df.withColumn(
        "fecha_inicio",
        F.col("fecha_evento")
    )

    df = df.withColumn(
        "fecha_fin",
        F.lead("fecha_evento").over(window_empleado)
    )

    df = df.withColumn(
        "es_actual",
        F.when(
            F.col("fecha_fin").isNull(),
            F.lit(True)
        )
        .otherwise(F.lit(False))
    )

    return df


# ================================================================
# EMPLEADOS QUARANTINE
# ================================================================

@dp.materialized_view(
    name="electrocasa.silver.empleados_quarantine",
    comment="Eventos de empleados rechazados por reglas de calidad"
)
def empleados_quarantine():

    df = spark.read.table(
        "electrocasa.bronze.empleados_bronze"
    )

    df = normalizar_tipo_evento(df)

    df = df.withColumn(
        "dni",
        F.trim(F.col("dni").cast("string"))
    )

    df = df.withColumn(
        "fecha_evento",
        F.to_date(F.col("fecha_evento"))
    )

    df = df.withColumn(
        "salario",
        F.col("salario").cast("double")
    )

    # ============================================================
    # REGISTROS RECHAZADOS
    # ============================================================

    df = df.filter(
        F.col("dni").isNull()
    )

    return (
        df
        .withColumn(
            "motivo_rechazo",
            F.lit("dni_nulo")
        )
        .withColumn(
            "fecha_rechazo",
            F.current_timestamp()
        )
    )