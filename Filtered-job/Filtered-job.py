import sys
import time
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import col,when,substring


#--------------------------------------------------
# Argumentos
#--------------------------------------------------

args=getResolvedOptions(
    sys.argv,
    ['JOB_NAME']
)

#--------------------------------------------------
# Contexto Glue/Spark
#--------------------------------------------------

sc=SparkContext() #Iniciar SparkContext
glueContext=GlueContext(sc) #Levantar Spark dentro de GlueContext
spark=glueContext.spark_session # Iniciar la sesión de Spark dentro del glueContext
job=Job(glueContext) #Levantar glueContext en el Job

job.init(
    args['JOB_NAME'],
    args
)
#--------------------------------------------------
# Ruta S3:
#--------------------------------------------------
s3_path = f"s3://bucket-bootcamp-bronze-0001/scripts-py/transactions/cards_transactions.csv"


#--------------------------------------------------
# Leer CSV DIRECTO como DataFrame
# (Sin DynamicFrame)
#--------------------------------------------------

df = ( spark.read.option("header",True)
                 .option("inferSchema",True)
                 .csv(s3_path)
)

#--------------------------------------------------
# Transformaciones
#--------------------------------------------------

df = ( df.filter(col("City")=="Madrid")
            .withColumn("valor_alto",when(col("amount") >100,True).otherwise(False))
            .withColumn("ultimos_digitos",substring(col("card_number"),-4,4))
)

df.show()


#--------------------------------------------------
# Write parquet
#--------------------------------------------------

bucket="bucket-bootcamp-silver-0001"

run_id=str(int(time.time()))
temp_prefix=f"tmp/transactions/{run_id}/"
final_key="scripts-py/transactions-outputs/transactions_final.parquet"

( df.coalesce(1).write.mode("overwrite")
                      .option("compression","uncompressed")
                      .parquet(f"s3://{bucket}/{temp_prefix}")
)

#--------------------------------------------------
# Renombrar parquet
#--------------------------------------------------

s3=boto3.client("s3")

response=s3.list_objects_v2( Bucket=bucket,Prefix=temp_prefix ) # Lista los objetos con los parámetros ingresados

for obj in response.get("Contents",[]):

    key=obj["Key"]

    if key.endswith(".parquet"):

        s3.copy_object(
            Bucket=bucket,
            CopySource={
                "Bucket":bucket,
                "Key":key
            },
            Key=final_key
        )

        break

#--------------------------------------------------
# Limpieza temp
#--------------------------------------------------

for obj in response.get("Contents",[]):

    s3.delete_object(
        Bucket=bucket,
        Key=obj["Key"]
    )

job.commit()