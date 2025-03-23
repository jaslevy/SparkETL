import sys
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import when, col, count,  round
#feel free to def new functions if you need

def create_dataframe(filepath, format, spark):
    """
    Create a spark df given a filepath and format.

    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session

    :return: the spark df uploaded
    """
    if format == "json":
        spark_df = spark.read.json(filepath)
    elif format == "csv":
        spark_df = spark.read.csv(filepath, header=True, inferSchema=True)
    else:
        raise ValueError("Only import json or csv files")

    return spark_df


def transform_nhis_data(nhis_df):
    """
    Transform df elements

    :param nhis_df: spark df
    :return: spark df, transformed df
    """
    nhis_df = nhis_df.withColumn(
        "_AGEG5YR",
        when(col("AGE_P").between(18, 24), 1.0)
        .when(col("AGE_P").between(25, 29), 2.0)
        .when(col("AGE_P").between(30, 34), 3.0)
        .when(col("AGE_P").between(35, 39), 4.0)
        .when(col("AGE_P").between(40, 44), 5.0)
        .when(col("AGE_P").between(45, 49), 6.0)
        .when(col("AGE_P").between(50, 54), 7.0)
        .when(col("AGE_P").between(55, 59), 8.0)
        .when(col("AGE_P").between(60, 64), 9.0)
        .when(col("AGE_P").between(65, 69), 10.0)
        .when(col("AGE_P").between(70, 74), 11.0)
        .when(col("AGE_P").between(75, 79), 12.0)
        .when(col("AGE_P").between(80, 99), 13.0) 
        .otherwise(14.0)
    )

    nhis_df = nhis_df.withColumn(
        "_IMPRACE",
        when((col("MRACBPI2") == 1) & (col("HISPAN_I") == 12), 1.0)  # White, Non-Hispanic
        .when((col("MRACBPI2") == 2) & (col("HISPAN_I") == 12), 2.0)  # Black, Non-Hispanic
        .when((col("MRACBPI2").isin(6, 7, 12)) & (col("HISPAN_I") == 12), 3.0)  # Asian, Non-Hispanic
        .when((col("MRACBPI2") == 3) & (col("HISPAN_I") == 12), 4.0)  # AI/AN, Non-Hispanic
        .when(col("HISPAN_I") != 12, 5.0)  # Hispanic Any Race
        .when((col("MRACBPI2").isin(16, 17)) & (col("HISPAN_I") == 12), 6.0)  # Other, Non-Hispanic
        .otherwise(6.0)
    )

    return nhis_df

def report_summary_stats(joined_df, output_path="report.txt"):
    with open(output_path, "w") as f:
        race_labels = {
            1.0: "White, Non-Hispanic",
            2.0: "Black, Non-Hispanic",
            3.0: "Asian, Non-Hispanic",
            4.0: "American Indian/Alaskan Native, Non-Hispanic",
            5.0: "Hispanic",
            6.0: "Other race, Non-Hispanic"
        }

        age_ranges = {
            1.0: "18–24", 2.0: "25–29", 3.0: "30–34", 4.0: "35–39", 5.0: "40–44",
            6.0: "45–49", 7.0: "50–54", 8.0: "55–59", 9.0: "60–64", 10.0: "65–69",
            11.0: "70–74", 12.0: "75–79", 13.0: "80-99", 14.0: "Unknown"
        }

        f.write("DIBEV1 Prevalence by Race (_IMPRACE):\n")
        race_stats = (
            joined_df.groupBy("_IMPRACE")
            .agg(
                count("*").alias("Total"),
                count(when(col("DIBEV1") == 1, True)).alias("Diabetes_Count")
            )
            .withColumn("Prevalence (%)", round((col("Diabetes_Count") / col("Total")) * 100, 2))
            .orderBy("_IMPRACE")
            .collect()
        )
        for row in race_stats:
            label = race_labels.get(row["_IMPRACE"], f"Unknown ({row['_IMPRACE']})")
            f.write(f"   - {label}: {row['Prevalence (%)']}% ({row['Diabetes_Count']} out of {row['Total']})\n")

        f.write("\n DIBEV1 Prevalence by Gender (SEX):\n")
        gender_stats = (
            joined_df.groupBy("SEX")
            .agg(
                count("*").alias("Total"),
                count(when(col("DIBEV1") == 1, True)).alias("Diabetes_Count")
            )
            .withColumn("Prevalence (%)", round((col("Diabetes_Count") / col("Total")) * 100, 2))
            .orderBy("SEX")
            .collect()
        )
        for row in gender_stats:
            label = "Male" if row["SEX"] == 1.0 else "Female"
            f.write(f"   - {label}: {row['Prevalence (%)']}% ({row['Diabetes_Count']} out of {row['Total']})\n")

        f.write("\n DIBEV1 Prevalence by Age Group (_AGEG5YR):\n")
        age_stats = (
            joined_df.groupBy("_AGEG5YR")
            .agg(
                count("*").alias("Total"),
                count(when(col("DIBEV1") == 1, True)).alias("Diabetes_Count")
            )
            .withColumn("Prevalence (%)", round((col("Diabetes_Count") / col("Total")) * 100, 2))
            .orderBy("_AGEG5YR")
            .collect()
        )
        for row in age_stats:
            label = age_ranges.get(row["_AGEG5YR"], f"Code {row['_AGEG5YR']}")
            f.write(f"   - Age {label}: {row['Prevalence (%)']}% ({row['Diabetes_Count']} out of {row['Total']})\n")



def join_data(brfss_df, nhis_df):
    joined_df = brfss_df.join(
        nhis_df, 
        on=["_AGEG5YR", "SEX", "_IMPRACE"],  
        how="inner"  
    )
    joined_df = joined_df.dropna()
    return joined_df


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('nhis', type=str, default=None, help="brfss filename")
    arg_parser.add_argument('brfss', type=str, default=None, help="nhis filename")
    arg_parser.add_argument('-o', '--output', type=str, default=None, help="output path(optional)")

    args = arg_parser.parse_args()
    if not args.nhis or not args.brfss:
        arg_parser.usage = arg_parser.format_help()
        arg_parser.print_usage()
    else:
        brfss_filename = args.nhis
        nhis_filename = args.brfss

        spark = SparkSession.builder.getOrCreate()

        brfss_df = create_dataframe(brfss_filename, 'json', spark)
        nhis_df = create_dataframe(nhis_filename, 'csv', spark)

        nhis_df = transform_nhis_data(nhis_df)
        joined_df = join_data(brfss_df, nhis_df)

        report_summary_stats(joined_df, output_path="summary_stats.txt")

        if args.output:
            selected_columns = ["_AGEG5YR", "SEX", "_LLCPWT", "DIBEV1", "_IMPRACE"]
            joined_df.select(*selected_columns).write.csv(args.output, mode="overwrite", header=True)

        spark.stop()