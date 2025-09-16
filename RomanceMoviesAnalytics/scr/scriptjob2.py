import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, coalesce, explode, to_date, date_format, monotonically_increasing_id,
    concat, split, lit
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc           = SparkContext()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session
job          = Job(glueContext)
job.init(args['JOB_NAME'], args)

combined_path         = "s3://sprint5-bucket/refined/combined/2/"     
out_dim_movie         = "s3://sprint5-bucket/refined/dim_movie/"
out_dim_genero        = "s3://sprint5-bucket/refined/dim_genero/"
out_dim_pais          = "s3://sprint5-bucket/refined/dim_pais/"
out_dim_diretor       = "s3://sprint5-bucket/refined/dim_diretor/"
out_dim_data          = "s3://sprint5-bucket/refined/dim_data/"
out_fact_movie_rating = "s3://sprint5-bucket/refined/fact_movie_rating/"

df = spark.read.parquet(combined_path)

df = df.withColumn(
    "dt",
    coalesce(
        to_date(col("release_date"), "yyyy-MM-dd"),
        to_date(concat(col("release_year").cast("string"), lit("-01-01")), "yyyy-MM-dd")
    )
)

dim_movie = (
    df.select(
        col("id").alias("movie_id"),
        col("tituloprincipal"),
        col("titulooriginal"),
        col("tempominutos")
    )
    .dropDuplicates(["movie_id"])
)
dim_movie.write.mode("overwrite").parquet(out_dim_movie)

dim_genero = (
    df
    .select(
        col("id").alias("movie_id"),
        explode(col("generos_csv")).alias("genero")
    )
    .dropDuplicates(["movie_id", "genero"])
)
dim_genero.write.mode("overwrite").parquet(out_dim_genero)

dim_pais = (
    df
    .where(col("origin_country").isNotNull())   
    .select(
        col("id").alias("movie_id"),
        explode(col("origin_country")).alias("country_code")
    )
    .dropDuplicates(["movie_id", "country_code"])
)
dim_pais.write.mode("overwrite").parquet(out_dim_pais)


# ------------------- Dimensão Diretor ---------------------------
dim_diretor = (
    df
    .withColumn("crew_member", explode(col("crew")))
    .filter(col("crew_member.job") == "Director")
    .select(
        col("crew_member.id")   .alias("director_id"),
        col("crew_member.name") .alias("nome_diretor"),
        col("crew_member.gender"),
        col("crew_member.known_for_department").alias("known_for_department")
    )
    .dropDuplicates(["director_id"])
)
dim_diretor.write.mode("overwrite").parquet(out_dim_diretor)

dim_data = (
    df.select("dt")
      .withColumn("date_key", date_format(col("dt"), "yyyyMMdd").cast("int"))
      .withColumn("ano",  date_format(col("dt"), "yyyy").cast("int"))
      .withColumn("mes",  date_format(col("dt"), "MM").cast("int"))
      .withColumn("dia",  date_format(col("dt"), "dd").cast("int"))
      .dropDuplicates(["date_key"])
)
dim_data.write.mode("overwrite").parquet(out_dim_data)

# ------------------- Fato Movie Rating --------------------------
fact_movie_rating = (
    df
    .withColumn("crew_member", explode(col("crew")))
    .filter(col("crew_member.job") == "Director")
    .select(
        monotonically_increasing_id().alias("fact_id"),
        col("id").alias("movie_id"),
        col("crew_member.id").alias("director_id"),
        date_format(col("dt"), "yyyyMMdd").cast("int").alias("date_key"),
        col("notamedia"),
        col("numerovotos")
    )
)
fact_movie_rating.write.mode("overwrite").parquet(out_fact_movie_rating)


job.commit()
