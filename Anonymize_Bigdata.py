from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2

# Initialize a Spark session
spark = SparkSession.builder.appName("CSV").getOrCreate()

# Set log level to INFO for more detailed logs (optional)
spark.sparkContext.setLogLevel("INFO")

# Read the CSV file
df = spark.read.csv('/Users/aishwaryatondepu/Downloads/large_dataset.csv', header=True)

# Anonymize the first_name, last_name, and email columns
df_anonymized = df.withColumn(
    'first_name', sha2('first_name', 256)
).withColumn(
    'last_name', sha2('last_name', 256)
).withColumn(
    'email', sha2('email', 256)
)

# Coalesce the DataFrame to a single partition to ensure a single output file
df_anonymized = df_anonymized.coalesce(1)

# Save the anonymized DataFrame to a new CSV file
df_anonymized.write.csv('/Users/aishwaryatondepu/Desktop/Assessment', header=True, mode='overwrite')

# Display the anonymized DataFrame (optional, for debugging)
df_anonymized.show()