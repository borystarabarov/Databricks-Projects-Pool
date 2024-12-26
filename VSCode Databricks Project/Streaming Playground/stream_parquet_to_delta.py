# Import required libraries
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import StructType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Parquet to Delta Stream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

try:
    # Define input and checkpoint paths
    input_path = "abfss://external-location@databricksdevstgacc.dfs.core.windows.net/files/"
    checkpoint_path = "abfss://external-location@databricksdevstgacc.dfs.core.windows.net/checkpoint/stream_test"

    # First, read existing parquet files to infer schema
    sample_df = spark.read.parquet(input_path)
    if sample_df.rdd.isEmpty():
        raise Exception("No files found in the input path to infer schema")
    
    inferred_schema = sample_df.schema

    # Read the streaming parquet files with the inferred schema
    stream_df = spark.readStream \
        .format("parquet") \
        .schema(inferred_schema) \
        .option("maxFilesPerTrigger", 1) \
        .load(input_path)

    # Write the stream to Delta table
    query = stream_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .table("dev_catalog.default.stream_test")

    # Add stream monitoring
    print("Stream started. Monitoring for new files...")
    
    # Wait for the streaming query to terminate
    query.awaitTermination()

except Exception as e:
    print(f"An error occurred: {str(e)}")
    if 'query' in locals():
        query.stop()
    raise 