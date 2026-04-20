import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *

import boto3
import time


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#args = getResolvedOptions(sys.argv, ['JOB_NAME','JOB_RUN_ID'])

# Ruta S3:
s3_path = f"s3://bucket-bootcamp-bronze-0001/scripts-py/transactions/cards_transactions.csv"

#Iniciar argumento
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Crear DYF
transactions_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ",",
        "inferSchema": True
    }
)

#Convertir DYF a DF
transactions_df = transactions_dyf.toDF()

#Filtro del DF
transactions_df = transactions_df.filter(col("City") == "Madrid")

#Mostrar filtro
transactions_df.show()

#Columnas Calculadas
transactions_df = transactions_df.withColumn('valor_alto' ,when(col("amount") > 100.0, True ).otherwise(False)
                    ).withColumn('ultimos_digitos',substring(col("card_number"),-4,4)
                    )

#Mostrar cambios
transactions_df.show()

#Convertir DF a DYF                   
final_dyf = DynamicFrame.fromDF(transactions_df,glueContext,"final_dyf")


bucket = "bucket-bootcamp-silver-0001"
run_id = str(int(time.time()))
#run_id = args['JOB_RUN_ID']
# Ruta temporal única por ejecución
temp_prefix = f"tmp/transactions/{run_id}/"
# Nombre final del parquet
final_key = "scripts-py/transactions-outputs/transactions_final.parquet"

# Convertimos a DF y forzamos un solo archivo
# ==========================================
final_df = final_dyf.toDF().coalesce(1)

# Ahora usamos DataFrame.write porque permite:
# - uncompressed
# - control para renombrar después
# ==========================================
final_df.write \
    .mode("overwrite") \
    .option("compression", "uncompressed") \
    .parquet(
        f"s3://{bucket}/{temp_prefix}"
    )

# Renombrar el part-xxxxx.parquet
# a transactions_final.parquet
# ==========================================
s3 = boto3.client("s3")

response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=temp_prefix
)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".parquet"):
        s3.copy_object(
            Bucket=bucket,
            CopySource={
                "Bucket": bucket,
                "Key": key
            },
            Key=final_key
        )
        print(
            f"Archivo final generado: s3://{bucket}/{final_key}"
        )
        break

for obj in response.get("Contents", []):

    s3.delete_object(
        Bucket=bucket,
        Key=obj["Key"]
    )

#Generar S3 parquet.
#write_final_dyf =glueContext.write_dynamic_frame.from_options(
#                    frame = final_dyf,
#                    connection_type = "S3", 
#                    connection_options={"path":"s3://bucket-bootcamp-silver-0001/scripts-py/transactions-outputs/"}, 
#                    format="parquet", 
#                    transformation_ctx="write_final_dyf"
#                )

job.commit()