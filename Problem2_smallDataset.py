# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2
import pandas as pd

# COMMAND ----------

# Initialize a Spark session
spark = SparkSession.builder.appName("CSV").getOrCreate()

# COMMAND ----------

# Sample data
data = {
    'first_name': ['John', 'Jane', 'Alice', 'Bob'],
    'last_name': ['Doe', 'Smith', 'Johnson', 'Brown'],
    'address': ['123 Main St', '456 Maple Ave', '789 Oak Dr', '101 Pine Ln'],
    'date_of_birth': ['1990-01-01', '1985-05-15', '1978-12-30', '2000-07-04']
}

# COMMAND ----------

# Create a DataFrame
df = pd.DataFrame(data)
spark_df = spark.createDataFrame(df)

# COMMAND ----------

# Coalesce to a single partition and write the DataFrame to a single CSV file
spark_df.coalesce(1).write.csv('/dbfs/tmp/my_directory/sample_data', header=True, mode='overwrite')

# COMMAND ----------

# Read the CSV file
df = spark.read.csv('/dbfs/tmp/my_directory/sample_data/*.csv', header=True)

# COMMAND ----------

# Anonymize the first_name, last_name, and address columns
df_anonymized = df.withColumn(
    'first_name', sha2('first_name', 256)
).withColumn(
    'last_name', sha2('last_name', 256)
).withColumn(
    'address', sha2('address', 256)
)

# COMMAND ----------

# Show the anonymized DataFrame
df_anonymized.show()

# COMMAND ----------

# Save the anonymized DataFrame to a new CSV file
df_anonymized.write.csv('/dbfs/tmp/my_directory/anonymized_data.csv', header=True, mode='overwrite')

# COMMAND ----------

display(df_anonymized)
