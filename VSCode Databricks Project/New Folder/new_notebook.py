# Import necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

# Define the file path
file_path = "abfss://external-location@databricksdevstgacc.dfs.core.windows.net/files/"

# Read the parquet file into a dataframe
df = spark.read.parquet(file_path)

# Show the dataframe
df.show()

display(df)
