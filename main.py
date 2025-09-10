
from config.config import (column_list
                           , configuration
                           , text_input
                           , json_input)

from utils.spark_util import (data_schema
                              , define_udfs)

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("AWS_Spark_Unstructured_Data")
             .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.4.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             # .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
             # .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
             .config("spark.hadoop.fs.s3a.aws.credentials.provides"
                     , "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())

    udfs = define_udfs()

    job_bulletins_df = (spark.readStream.format("text")
                        .option("wholetext", "true")
                        .load(text_input))

    json_df = spark.readStream.json(json_input, schema=data_schema, multiLine=True)

    job_bulletins_df = job_bulletins_df.withColumn("file_name",
                                                   regexp_replace(udfs["extract_file_name_udf"]("value"), r'\r', ' '))
    job_bulletins_df = job_bulletins_df.withColumn("value", regexp_replace(regexp_replace("value", r'\n', ' '), r'\r', ' '))
    job_bulletins_df = job_bulletins_df.withColumn("position",
                                                   regexp_replace(udfs["extract_position_udf"]("value"), r'\r', ' '))
    job_bulletins_df = job_bulletins_df.withColumn("salary_start",
                                                   udfs["extract_salary_udf"]("value").getField("salary_start"))
    job_bulletins_df = job_bulletins_df.withColumn("salary_end",
                                                   udfs["extract_salary_udf"]("value").getField("salary_end"))
    job_bulletins_df = job_bulletins_df.withColumn("start_date", udfs["extract_start_date_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("end_date", udfs["extract_end_date_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("classcode", udfs["extract_classcode_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("req", udfs["extract_requirements_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("notes", udfs["extract_notes_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("duties", udfs["extract_duties_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("selection", udfs["extract_selection_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("experience_length", udfs["extract_experience_length_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("education_length", udfs["extract_education_length_udf"]("value"))
    job_bulletins_df = job_bulletins_df.withColumn("application_location", udfs["extract_application_location_udf"]("value"))

    job_bulletins_df = job_bulletins_df.select(*column_list)

    json_df = json_df.select(*column_list)

    union_df = job_bulletins_df.union(json_df)

    query = (union_df
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start()
             )

    query.awaitTermination()