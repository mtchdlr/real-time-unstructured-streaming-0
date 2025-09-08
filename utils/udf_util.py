import re
from datetime import datetime
def extract_file_name(file_content):
    file_content = file_content.strip()
    position = file_content.split("\n")[0]

    return position


def extract_position(file_content):
    pass


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
    pass


def extract_notes(file_content):
    pass


def extract_duties(file_content):
    pass


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
    pass


def extract_experience_length(file_content):
    pass


def extract_education_length(file_content):
    pass


def extract_application_location(file_content):
    pass
