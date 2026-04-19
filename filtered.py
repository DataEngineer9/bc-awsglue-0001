import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

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

transactions_df = transactions_dyf.toDF()

#Filtro del DF
transactions_df = transactions_df.filter(col("City") == "Madrid")

#Mostrar tabla filtrada
filtered_df.show()

#Columnas Calculadas
transactions_df = transactions_df.withColumn('valor_alto' ,when(col("amount") > 100.0, True ).otherwise(False)
                    ).withColumn('ultimos_digitos',substring(col("card_number"),-4,4)
                    )

#Mostrar la tabla
transactions_df.show()

#Convertir DF a DYF                   
final_dyf = DynamicFrame.fromDF(transactions_df,glueContext,"final_dyf")


job.commit()