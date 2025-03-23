import pandas as pd

df_spark = pd.read_csv("./data/test_script/part-00000-e739cc37-5f68-44d6-900c-78dad31cfa9e-c000.csv")
df_manual = pd.read_csv("./data/joined_test.csv/part-00000-e7c74796-b743-4c75-9fc5-af9d09377e4c-c000.csv")

df_spark = df_spark.astype(float)
df_manual = df_manual.astype(float)

print(df_spark.equals(df_manual))  

diff = df_spark.compare(df_manual)
print(diff)