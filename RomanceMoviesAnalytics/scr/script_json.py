import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job_name    = args["JOB_NAME"]

raw_path    = "s3://sprint5-bucket/Raw/TMDB/JSON/2025/05/23/"   # pasta-mãe do RAW
trusted_root= "s3://sprint5-bucket/trusted/TMDB/JSON/"          # pasta-mãe TRUSTED

sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
job.init(job_name, args)

df_raw = (
    spark.read
         .option("multiline", "true")    
         .json(raw_path)
)


df_clean = (
    df_raw
    .filter((col("details.erro").isNull()) | (col("details.erro") == ""))
    .filter(col("details.id").isNotNull())
    .dropDuplicates()
)

df_fixed = (
    df_clean
    .withColumn("details.id",           col("details.id").cast(IntegerType()))
    .withColumn("details.vote_average", col("details.vote_average").cast(FloatType()))
    .withColumn("details.vote_count",   col("details.vote_count").cast(IntegerType()))
)

today = datetime.utcnow().strftime("%Y-%m-%d")   
output_path = f"{trusted_root}dt={today}/"

(
    df_fixed
    .write
    .mode("overwrite") 
    .parquet(output_path)
)

job.commit()
