from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName("joined_test").getOrCreate()

manual_schema = StructType([
    StructField("_AGEG5YR", FloatType(), True),
    StructField("SEX", FloatType(), True),       
    StructField("_IMPRACE", FloatType(), True),  
    StructField("_LLCPWT", IntegerType(), True), 
    StructField("MRACBPI2", IntegerType(), True), 
    StructField("HISPAN_I", IntegerType(), True), 
    StructField("AGE_P", IntegerType(), True),   
    StructField("DIBEV1", IntegerType(), True)     
])

manual_data = [
    (6.0, 1.0, 1.0, 2, 1, 12, 45, 1),  
    (10.0, 2.0, 5.0, 2, 1, 0, 67, 1),  
    (1.0, 2.0, 1.0, 1, 1, 12, 19, 2)  
]


expected_df = spark.createDataFrame(manual_data, schema=manual_schema)

expected_df = expected_df.dropna()


expected_df.coalesce(1).write.csv("./data/joined_test.csv", mode="overwrite", header=True)

spark.stop()
