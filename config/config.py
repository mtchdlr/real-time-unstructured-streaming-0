import os
from typing import (Dict
                    , List)

configuration: Dict[str, str] = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID")
    , "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY")
}

column_list: List[str] = ["file_name", "start_date", "end_date", "salary_start", "salary_end", "classcode"
                          , "req", "notes", "duties", "selection", "experience_length"
                          , "education_length", "application_location"]