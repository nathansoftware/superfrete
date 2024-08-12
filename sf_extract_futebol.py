from pyspark.sql import SparkSession
import requests
import pandas as pd

# Inicializar o SparkSession com Delta Lake e BigQuery
spark = SparkSession.builder \
    .appName("Futebol") \
    .getOrCreate()

# 1. Ler dados da API
api_url = "https://api.api-futebol.com.br/v1/"
response = requests.get(api_url)
data = response.json()

# Converter dados para DataFrame do Pandas
df = pd.json_normalize(data)

# Converter o DataFrame do Pandas para DataFrame do Spark
spark_df = spark.createDataFrame(df)

# 2. Escrever no Google Cloud Storage no formato Delta
gcs_bucket = "superFrete-projeto"
gcs_path = "dados-futebol/futebol"
spark_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"gs://{gcs_bucket}/{gcs_path}")

# 3. Ler dados do Google Cloud Storage no formato Delta
f_df = spark.read.format("delta").load(f"gs://{gcs_bucket}/{gcs_path}")

# 4. Escrever no BigQuery
bq_table = "bronze_futebol.futebol"
f_df.write \
    .format("bigquery") \
    .option("table", bq_table) \
    .mode("overwrite") \
    .save()

print("Dados processados e salvos com sucesso.")
