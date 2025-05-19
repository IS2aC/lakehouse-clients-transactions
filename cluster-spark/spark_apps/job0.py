# here is the job0 for testting connectivity to minio from our spark's cluster
# job0.py --> it's successfully connected to minio and read the csv
# #get ready to make launch a spark job from astro-airflow. 

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os 

load_dotenv('.env')

# Création de la SparkSession avec les options nécessaires pour accéder à MinIO via S3A
spark = SparkSession.builder \
    .appName("Lire fichier CSV depuis MinIO") \
    .config("spark.hadoop.fs.s3a.access.key", f"{os.getenv('MINIO_ACCESS_KEY')}") \
    .config("spark.hadoop.fs.s3a.secret.key", f"{os.getenv('MINIO_SECRET_KEY')}") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()

# Lecture du fichier CSV depuis MinIO
df = spark.read \
    .option("header", "true") \
    .csv("s3a://bucket1/exemple_minio.csv")


df_pandas  =  df.toPandas()
print(df_pandas)
