import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower, trim, split, when
from pyspark.sql.types import IntegerType, FloatType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = "s3://sprint5-bucket/Raw/Local/CSV/Movies/2025/05/19/movies.csv"
output_path = "s3://sprint5-bucket/trusted/csv/"

df = spark.read.option("header", "true").option("sep", "|").csv(input_path)

# correção do nome da coluna
df = df.withColumnRenamed("tituloPincipal", "tituloPrincipal")
# tratando dados nulos e duplicados
df = df.replace('\\N', None)
df = df.dropna(how="all")
df = df.dropDuplicates()
# transformando colunas numericas em int ou float
df = df.withColumn("anoNascimento", col("anoNascimento").cast("int"))
df = df.withColumn("anoFalecimento", col("anoFalecimento").cast("int"))
df = df.withColumn("anoLancamento", col("anoLancamento").cast("int"))
df = df.withColumn("tempoMinutos", col("tempoMinutos").cast("int"))
df = df.withColumn("notaMedia", col("notaMedia").cast("float"))
df = df.withColumn("numeroVotos", col("numeroVotos").cast("int"))
# Tratamento das colunas do tipo string
df = df.withColumn("profissao", lower(trim(col("profissao"))))
df = df.withColumn("genero", lower(trim(col("genero"))))
df = df.withColumn("generoArtista", lower(trim(col("generoArtista"))))
df = df.withColumn("nomeArtista", trim(col("nomeArtista")))
df = df.withColumn("personagem", trim(col("personagem")))
df = df.withColumn("tituloPrincipal", trim(col("tituloPrincipal")))
df = df.withColumn("tituloOriginal", trim(col("tituloOriginal")))
# Colunas que possuem mais de um valor transfomei em lista
df = df.withColumn("profissao", split(col("profissao"), ","))
df = df.withColumn("titulosMaisConhecidos", split(col("titulosMaisConhecidos"), ","))
df = df.withColumn("genero", split(col("genero"), ","))

df.write.mode("overwrite").parquet(output_path)
job.commit()