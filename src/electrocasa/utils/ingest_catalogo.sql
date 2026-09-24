%sql
COPY INTO electrocasa.bronze.catalogo_bronze
FROM (
    SELECT
        categoria,
        marca,
        nombre_producto,
        try_cast(precio_lista AS DOUBLE) AS precio_lista,
        producto_id,
        CAST(NULL AS STRING) AS _rescued_data,
        current_timestamp() AS fecha_ingestion,
        'catalogo_productos.json' AS sistema_origen,
        'catalogo_inicial' AS batch_id
    FROM '/Volumes/electrocasa/bronze/landing/catalogo/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS (
    'multiLine' = 'true'
)
COPY_OPTIONS (
    'mergeSchema' = 'true'
);