import re
from datetime import datetime


from pyspark.sql.types import (StructType
                               , StructField
                               , StringType
                               , DoubleType
                               , DateType)
from pyspark.sql.functions import udf


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


def define_udfs():
    return {
        "extract_file_name_udf": udf(extract_file_name, StringType()),
        "extract_position_udf": udf(extract_position, StringType()),
        "extract_salary_udf": udf(extract_salary, StructType([
            StructField("salary_start", DoubleType(), True),
            StructField("salary_end", DoubleType(), True),
        ])),
        "extract_start_date_udf": udf(extract_start_date, DateType()),
        "extract_end_date_udf": udf(extract_end_date, DateType()),
        "extract_classcode_udf": udf(extract_class_code, StringType()),
        "extract_requirements_udf": udf(extract_requirements, StringType()),
        "extract_notes_udf": udf(extract_notes, StringType()),
        "extract_duties_udf": udf(extract_duties, StringType()),
        "extract_selection_udf": udf(extract_selection, StringType()),
        "extract_experience_length_udf": udf(extract_experience_length, StringType()),
        "extract_education_length_udf": udf(extract_education_length, StringType()),
        "extract_application_location_udf": udf(extract_application_location, StringType()),
    }


def extract_file_name(file_content):
    file_content = file_content.strip()
    position = file_content.split("\n")[0]

    return position


def extract_position(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position


def extract_class_code(file_content):
    try:
        classcode_match = re.search(r'(Class Code:)\s+(\d+)', file_content)
        class_code = classcode_match.group(2) if classcode_match else None

        return class_code
    except Exception as e:
        raise ValueError(f"Error extracting class code: {e}")


def extract_salary(file_content):
    try:
        salary_pattern = r'\$(\d{1,3}(?:,\d{3})+).+?to.+\$(\d{1,3}(?:,\d{3})+)(?:\s+and\s+\$(\d{1,3}(?:,\d{3})+)\s+to\s+\$(\d{1,3}(?:,\d{3})+))?'
        salary_match = re.search(salary_pattern, file_content)

        if salary_match:
            salary_start = float(salary_match.group(1).replace(",", ""))
            salary_end = float(salary_match.group(4).replace(",", "")) if salary_match.group(4) \
                else salary_match.group(2).replace(",", "")
        else:
            salary_start, salary_end = None, None

        return salary_start, salary_end

    except Exception as e:
        raise ValueError(f"Error extracting salary: {e}")


def extract_requirements(file_content):
    try:
        req_match = re.search(r"(REQUIREMENTS?/\S?MINIMUM QUALIFICATION?)(.*)(PROCESS NOTES?)", file_content, re.DOTALL)
        req = req_match.group(2).strip() if req_match else None

        return req

    except Exception as e:
        raise ValueError(f"Error extracting requirements: {e}")


def extract_notes(file_content):
    try:
        notes_match = re.search(r"(NOTES?):(.*?)(?=DUTIES)", file_content, re.DOTALL | re.IGNORECASE)
        notes = notes_match.group(2).strip() if notes_match else None

        return notes

    except Exception as e:
        raise ValueError(f"Error extracting notes: {e}")


def extract_duties(file_content):
    try:
        duties_match = re.search(r"(DUTIES?):(.*?)(REQ[A-Z])", file_content, re.DOTALL)
        duties = duties_match.group(2).strip() if duties_match else None

        return duties

    except Exception as e:
        raise ValueError(f"Error extracting duties: {e}")


def extract_start_date(file_content):
    try:
        opendate_match = re.search(r'(Open [Dd]ate:)\s+(\d\d-\d\d-\d\d)', file_content)
        start_date = datetime.strptime(opendate_match.group(2), '%m-%d-%y')

        return start_date
    except Exception as e:
        raise ValueError(f"Error extracting start date: {e}")


def extract_end_date(file_content):
    try:
        end_date_match = re.search(
            r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBERN|NOVEMBER|DECEMBER)\S(\d{1,2},\s\d{4})'
            , file_content
        )
        end_date_group = end_date_match.group(2) if end_date_match else None
        end_date = datetime.strptime(end_date_group, '%B %d, %Y') if end_date_group else None

        return end_date

    except Exception as e:
        raise ValueError(f"Error extracting end date: {e}")

def extract_selection(file_content):
    try:
        sel_match = re.findall(r'([A-Z][a-z]+)(\s\.\s)+', file_content)
        sel = [z[0] for z in sel_match] if sel_match else None

        return sel

    except Exception as e:
        raise ValueError(f"Error extracting selection: {e}")


def extract_experience_length(file_content):
    try:
        exp_match = re.search(
            r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five)\s(years?)\s(of\sfull(-|\s)time)',
            file_content)
        exp = exp_match.group(1) if exp_match else None
        return exp
    except Exception as e:
        raise ValueError(f'Error extracting experience length: {e}')


def extract_education_length(file_content):
    try:
        edu_match = re.search(
            r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five)(\s|-)(years?)\s(college|university)',
            file_content)
        edu = edu_match.group(1) if edu_match else None
        return edu
    except Exception as e:
        raise ValueError(f'Error extracting education length: {e}')


def extract_application_location(file_content):
    try:
        app_loc_match = re.search(r'(Applications? will only be accepted on-?line)', file_content,
                                          re.IGNORECASE)
        app_loc = 'Online' if app_loc_match else 'Mail or In Person'
        return app_loc
    except Exception as e:
        raise ValueError(f'Error extracting application location: {e}')


def stream_writer(input_data, checkpoint_folder: str, output_path: str):
    return (input_data.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint_folder)
            .option("path", output_path)
            .outputMode("append")
            .trigger(processingTime="5 seconds")
            .start())