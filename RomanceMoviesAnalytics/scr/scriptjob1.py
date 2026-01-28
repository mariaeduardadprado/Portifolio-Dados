import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

trusted_csv_path  = "s3://sprint5-bucket/trusted/Local/CSV/"              
trusted_json_path = "s3://sprint5-bucket/trusted/TMDB/JSON/dt=2025-06-27/"      
refined_out       = "s3://sprint5-bucket/refined/combined/2/"

sc            = SparkContext()
glue_context  = GlueContext(sc)
spark         = glue_context.spark_session
job           = Job(glue_context)
job.init(job_name, args)

df_csv  = spark.read.parquet(trusted_csv_path)

df_json = spark.read.parquet(trusted_json_path)

df_csv_norm = (
    df_csv.select(
        col("id"),
        col("tituloprincipal"),
        col("titulooriginal"),
        col("anolancamento")      .alias("release_year"),
        col("tempominutos"),
        col("genero")             .alias("generos_csv"),
        col("notamedia"),
        col("numerovotos"),
        col("generoartista"),
        col("personagem"),
        col("nomeartista"),
        col("anonascimento"),
        col("anofalecimento"),
        col("profissao"),
        col("titulosmaisconhecidos")
    )
)

df_json_norm = (
    df_json.select(
        col("details.id")                   .alias("id"),
        col("details.title")                .alias("tituloprincipal"),
        col("details.original_title")       .alias("titulooriginal"),
        col("details.runtime")              .alias("tempominutos"),
        col("details.release_date")         .alias("release_date"), 
        col("details.vote_average")         .alias("notamedia"),
        col("details.vote_count")           .alias("numerovotos"),
        col("details.budget"),
        col("details.revenue"),
        col("details.adult"),
        col("details.status"),
        col("details.genres")               .alias("generos_json"),
        col("details.production_countries"),
        col("details.origin_country"),
        col("details.spoken_languages"),
        col("details.popularity"),
        col("details.tagline"),
        col("details.video"),
        col("details.imdb_id"),
        col("details.homepage"),
        col("credits.cast"),
        col("credits.crew")
    )
)

df_combined = (
    df_json_norm
    .unionByName(df_csv_norm, allowMissingColumns=True)
)


df_combined.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(refined_out)

job.commit()
