
from config.config import configuration
from utils.udf_util import *

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType
                               , StructField
                               , StringType
                               , DoubleType
                               , DateType)
from pyspark.sql.functions import udf


def define_udfs():
    return {
        "extract_file_name_urf": udf(extract_file_name, StringType()),
        "extract_position_urf": udf(extract_file_name, StringType()),
        "extract_salary_urf": udf(extract_file_name, StructType([
            StructField("salary_start", DoubleType(), True),
            StructField("salary_end", DoubleType(), True),
        ])),
        "extract_start_date_urf": udf(extract_start_date, DateType()),
        "extract_end_date_urf": udf(extract_end_date, DateType()),
        "extract_classcode_urf": udf(extract_class_code, StringType()),
        "extract_requirements_urf": udf(extract_requirements, StringType()),
        "extract_notes_urf": udf(extract_notes, StringType()),
        "extract_duties_udf": udf(extract_duties, StringType()),
        "extract_selection_udf": udf(extract_selection, StringType()),
        "extract_experience_length_udf": udf(extract_experience_length, StringType()),
        "extract_education_length_udf": udf(extract_education_length, StringType()),
        "extract_application_location_udf": udf(extract_application_location, StringType()),
    }

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("AWS_Spark_Unstructured_Data")
             .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.1,"
                    "com.amazonaws.aws-java-sdk:1.11.469")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
             .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
             .config("spark.hadoop.fs.s3a.aws.credentials.provides"
                     , "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())

    input_dir: str = "file://home/mtchdlr/real-time-unstructured-streaming-0/input"
    text_input: str = input_dir + "/input_text"
    json_input: str = input_dir + "/input_json"
    video_input: str = input_dir + "/input_video"
    image_input: str = input_dir + "/input_image"
    pdf_input: str = input_dir + "/input_pdf"
    csv_input: str = input_dir + "/input_csv"

    data_schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("position", StringType(), True),
        StructField("classcode", StringType(), True),
        StructField("salary_start", DoubleType(), True),
        StructField("salary_end", DoubleType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("req", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("duties", StringType(), True),
        StructField("selection", StringType(), True),
        StructField("experience_length", StringType(), True),
        StructField("education_length", StringType(), True),
        StructField("school_type", StringType(), True),
        StructField("application_location", StringType(), True),
    ])

    udfs = define_udfs()

    job_bulletins_df = (spark.readStream.format("text")
                        .option("wholetext", "true")
                        .load(text_input))
    
    job_bulletins_df.show()