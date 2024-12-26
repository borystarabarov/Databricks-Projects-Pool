# Import necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session with Delta support
spark = SparkSession.builder \
    .appName("Query Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Execute the query
query = """
SELECT *
FROM dev_catalog.default.stream_test
LIMIT 100
"""

# Run the query and create a DataFrame
result_df = spark.sql(query)

# Display the results
result_df.show() 