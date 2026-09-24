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