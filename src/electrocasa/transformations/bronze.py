from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="ventas_bronze",
    comment="Ventas originales ingeridas desde CSV mediante Auto Loader"
)
def ventas_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/electrocasa/bronze/landing/_schemas/ventas/"
        )
        .option("header", "true")
        .load("/Volumes/electrocasa/bronze/landing/ventas/")
    )

    return (
        df
        .withColumn("fecha_ingestion", F.current_timestamp())
        .withColumn(
            "sistema_origen",
            F.lit("ventas_sucursales.csv")
        )
        .withColumn(
            "batch_id",
            F.lit("ventas_inicial")
        )
    )

@dp.table(
    name="empleados_bronze",
    comment="Eventos originales de empleados ingeridos desde CSV mediante Auto Loader"
)
def empleados_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/electrocasa/bronze/landing/_schemas/empleados/"
        )
        .option("header", "true")
        .load("/Volumes/electrocasa/bronze/landing/empleados/")
    )

    return (
        df
        .withColumn("fecha_ingestion", F.current_timestamp())
        .withColumn(
            "sistema_origen",
            F.lit("empleados.csv")
        )
        .withColumn(
            "batch_id",
            F.lit("empleados_inicial")
        )
    )

@dp.table(
    name="resenas_bronze",
    comment="Reseñas de clientes ingeridas desde JSON mediante Auto Loader"
)
def resenas_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/electrocasa/bronze/landing/_schemas/resenas/"
        )
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/electrocasa/bronze/landing/resenas/")
    )

    return (
        df
        .withColumn("fecha_ingestion", F.current_timestamp())
        .withColumn(
            "sistema_origen",
            F.lit("resenas.json")
        )
        .withColumn(
            "batch_id",
            F.lit("resenas_inicial")
        )
    )

@dp.table(
    name="devoluciones_bronze",
    comment="Devoluciones originales ingeridas desde CSV mediante Auto Loader"
)
def devoluciones_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/electrocasa/bronze/landing/_schemas/devoluciones/"
        )
        .option("header", "true")
        .load("/Volumes/electrocasa/bronze/landing/devoluciones/")
    )

    return (
        df
        .withColumn("fecha_ingestion", F.current_timestamp())
        .withColumn(
            "sistema_origen",
            F.lit("devoluciones.csv")
        )
        .withColumn(
            "batch_id",
            F.lit("devoluciones_inicial")
        )
    )

@dp.table(
    name="tracking_bronze",
    comment="Tracking de envíos ingerido desde Azure SQL mediante Lakehouse Federation"
)
def tracking_bronze():

    return (
        spark.read.table(
            "electrocasa_tracking.dbo.TrackingEnvios"
        )
        .withColumn("fecha_ingestion", F.current_timestamp())
        .withColumn(
            "sistema_origen",
            F.lit("azure_sql_tracking")
        )
        .withColumn(
            "batch_id",
            F.lit("tracking_inicial")
        )
    )