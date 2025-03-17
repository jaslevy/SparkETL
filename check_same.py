import pandas as pd

df_spark = pd.read_csv("./data/test_script/part-00000-e9856aaa-4f44-431e-8d8f-eee10ad818ea-c000.csv")
df_manual = pd.read_csv("./data/joined_test.csv/part-00000-6fd5218b-99bb-4df1-b01c-662aaa059393-c000.csv")

df_spark = df_spark.astype(float)
df_manual = df_manual.astype(float)

print(df_spark.equals(df_manual))  

diff = df_spark.compare(df_manual)
print(diff)