from pyspark.sql import SparkSession
import sys

# Iniciamos Spark con configuración de memoria explícita
spark = SparkSession.builder \
    .appName("PostgresToLocal") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

print(">>> PASO 1: Detectando rango de IDs en la base de datos...")

try:
    # Leer el rango de IDs
    rango_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/intcomex") \
        .option("dbtable", "(SELECT MIN(ad_changelog_id) as min_id, MAX(ad_changelog_id) as max_id FROM ad_changelog) as rango") \
        .option("user", "adempiere") \
        .option("password", "adempiere") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # Recoger el resultado
    rango = rango_df.collect()
    
    if not rango or rango[0]['min_id'] is None:
        print(">>> [ERROR] No se encontraron datos en la tabla ad_changelog")
        sys.exit(1)
    
    res = rango[0]
    min_id = int(res['min_id'])
    max_id = int(res['max_id'])
    
    print(f">>> Rango detectado: {min_id} a {max_id}")
    print(f">>> Total de registros aproximados: {max_id - min_id + 1}")

    print(f">>> PASO 2: Leyendo datos con 100 particiones...")
    
    # Leer toda la tabla particionada
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/intcomex") \
        .option("dbtable", "ad_changelog") \
        .option("user", "adempiere") \
        .option("password", "adempiere") \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", "ad_changelog_id") \
        .option("lowerBound", str(min_id)) \
        .option("upperBound", str(max_id)) \
        .option("numPartitions", "100") \
        .option("fetchsize", "5000") \
        .load()

    print(">>> Datos cargados en memoria. Iniciando escritura...")
    
    # Contar registros (esto fuerza la lectura)
    total_registros = df.count()
    print(f">>> Total de registros leídos: {total_registros}")

    # Reparticionar y escribir
    output_path = "/home/iceberg/work-dir/minio_data/datos_65m_json"
    print(f">>> PASO 3: Balanceando datos y escribiendo a {output_path}...")
    
    df.repartition(100) \
      .write \
      .mode("overwrite") \
      .json(output_path)

    print(">>> [EXITO TOTAL] Los 100 archivos JSON se han creado correctamente.")
    print(f">>> Ubicación: {output_path}")

except Exception as e:
    print(f">>> [ERROR CRITICO] Ocurrió un problema: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    # Asegurarse de cerrar Spark correctamente
    spark.stop()
    print(">>> Spark detenido correctamente.")