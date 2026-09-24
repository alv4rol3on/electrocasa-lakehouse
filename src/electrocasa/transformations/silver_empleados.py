from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# NORMALIZAR TIPO DE EVENTO
# ============================================================

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


# ============================================================
# SILVER - EMPLEADOS HISTORIZADOS
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.empleados_silver",
    comment="Historial de empleados con historización de altas, transferencias, cambios salariales y bajas"
)
@dp.expect_or_drop(
    "dni_valido",
    "dni IS NOT NULL"
)
@dp.expect_or_drop(
    "fecha_evento_valida",
    "fecha_evento IS NOT NULL"
)
def empleados_silver():

    # --------------------------------------------------------
    # Leer Bronze
    # --------------------------------------------------------

    df = spark.read.table(
        "electrocasa.bronze.empleados_bronze"
    )

    # --------------------------------------------------------
    # Normalizar tipo de evento
    # --------------------------------------------------------

    df = normalizar_tipo_evento(df)

    # --------------------------------------------------------
    # Estandarizar DNI
    # --------------------------------------------------------

    df = df.withColumn(
        "dni",
        F.trim(
            F.col("dni").cast("string")
        )
    )

    # --------------------------------------------------------
    # Estandarizar fecha del evento
    # --------------------------------------------------------

    df = df.withColumn(
        "fecha_evento",
        F.to_date(
            F.col("fecha_evento")
        )
    )

    # --------------------------------------------------------
    # Estandarizar salario
    # --------------------------------------------------------

    df = df.withColumn(
        "salario",
        F.col("salario").cast("double")
    )

    # --------------------------------------------------------
    # Prioridad de eventos
    #
    # Se utiliza para resolver eventos del mismo día.
    #
    # Ejemplo:
    #   alta            -> 1
    #   transferencia   -> 2
    #   cambio_salario  -> 3
    #   baja            -> 4
    #
    # Así, si dos eventos ocurren el mismo día,
    # podemos determinar un orden determinista.
    # --------------------------------------------------------

    prioridad_evento = (
        F.when(
            F.col("tipo_evento") == "alta",
            1
        )
        .when(
            F.col("tipo_evento") == "transferencia",
            2
        )
        .when(
            F.col("tipo_evento") == "cambio_salario",
            3
        )
        .when(
            F.col("tipo_evento") == "baja",
            4
        )
        .otherwise(5)
    )

    # --------------------------------------------------------
    # Ventana de historización por empleado
    # --------------------------------------------------------

    window_empleado = (
        Window
        .partitionBy("dni")
        .orderBy(
            F.col("fecha_evento").asc(),
            prioridad_evento.asc()
        )
    )

    # --------------------------------------------------------
    # Fecha de inicio de cada versión
    # --------------------------------------------------------

    df = df.withColumn(
        "fecha_inicio",
        F.col("fecha_evento")
    )

    # --------------------------------------------------------
    # Fecha de fin:
    # corresponde a la fecha del siguiente evento
    # del mismo empleado.
    # --------------------------------------------------------

    df = df.withColumn(
        "fecha_fin",
        F.lead("fecha_evento").over(
            window_empleado
        )
    )

    # --------------------------------------------------------
    # Identificar la versión vigente
    # --------------------------------------------------------

    df = df.withColumn(
        "es_actual",
        F.when(
            F.col("fecha_fin").isNull(),
            F.lit(True)
        )
        .otherwise(
            F.lit(False)
        )
    )

    return df


# ============================================================
# QUARANTINE - EMPLEADOS
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.empleados_quarantine",
    comment="Eventos de empleados rechazados por reglas de calidad"
)
def empleados_quarantine():

    # --------------------------------------------------------
    # Leer Bronze
    # --------------------------------------------------------

    df = spark.read.table(
        "electrocasa.bronze.empleados_bronze"
    )

    # --------------------------------------------------------
    # Normalizar tipo de evento
    # --------------------------------------------------------

    df = normalizar_tipo_evento(df)

    # --------------------------------------------------------
    # Estandarizar DNI
    # --------------------------------------------------------

    df = df.withColumn(
        "dni",
        F.trim(
            F.col("dni").cast("string")
        )
    )

    # --------------------------------------------------------
    # Estandarizar fecha
    # --------------------------------------------------------

    df = df.withColumn(
        "fecha_evento",
        F.to_date(
            F.col("fecha_evento")
        )
    )

    # --------------------------------------------------------
    # Estandarizar salario
    # --------------------------------------------------------

    df = df.withColumn(
        "salario",
        F.col("salario").cast("double")
    )

    # --------------------------------------------------------
    # Registros rechazados
    #
    # 1. DNI nulo
    # 2. Fecha de evento nula/inválida
    # --------------------------------------------------------

    df = df.filter(
        F.col("dni").isNull()
        | F.col("fecha_evento").isNull()
    )

    # --------------------------------------------------------
    # Motivo del rechazo
    # --------------------------------------------------------

    df = df.withColumn(
        "motivo_rechazo",
        F.when(
            F.col("dni").isNull(),
            F.lit("dni_nulo")
        )
        .when(
            F.col("fecha_evento").isNull(),
            F.lit("fecha_evento_nula")
        )
        .otherwise(
            F.lit("otro")
        )
    )

    # --------------------------------------------------------
    # Fecha en que se envió a quarantine
    # --------------------------------------------------------

    return df.withColumn(
        "fecha_rechazo",
        F.current_timestamp()
    )