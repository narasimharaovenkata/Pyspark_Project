import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["SPARK_HOME"] = "C:/Users/ponna/OneDrive/Desktop/PYSPARK_TEST/PYSPARK_VERSIONS/spark-3.5.4-bin-hadoop3\spark-3.5.4-bin-hadoop3"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]= sys.executable

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySparkSample") \
    .master("local[1]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Sample Data (List of Tuples)
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
]

# Define Schema (Column Names)
columns = ["id", "name", "age"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show Data
print("Original DataFrame:")
df.show()

# Filter Data (People older than 28)
filtered_df = df.filter(col("age") > 28)

print("Filtered DataFrame (age > 28):")
filtered_df.show()

# Register DataFrame as a Temporary SQL Table
df.createOrReplaceTempView("people")

# Run SQL Query
sql_result = spark.sql("SELECT * FROM people WHERE age > 28")

print("SQL Query Result:")
sql_result.show()

# Stop Spark Session
spark.stop()
