from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName("manual_joined_test").getOrCreate()

manual_schema = StructType([
    StructField("_AGEG5YR", FloatType(), True),
    StructField("SEX", FloatType(), True),
    StructField("_LLCPWT", IntegerType(), True),
    StructField("DIBEV1", IntegerType(), True),
    StructField("_IMPRACE", FloatType(), True),
])


manual_data = [
    (6.0, 1.0, 2, 1, 1.0),
    (10.0, 2.0, 2, 1, 5.0),
    (1.0, 2.0, 1, 2, 1.0),
]

# Create DataFrame
expected_df = spark.createDataFrame(manual_data, schema=manual_schema)

# Save to CSV
expected_df.coalesce(1).write.csv("./data/joined_test.csv", mode="overwrite", header=True)

# Stop Spark
spark.stop()
