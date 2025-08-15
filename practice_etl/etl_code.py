import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(
        columns=["car_model", "year_of_manufacture", "price", "fuel"]
    )
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        dataframe = pd.concat(
            [
                dataframe,
                pd.DataFrame(
                    [
                        {
                            "car_model": car_model,
                            "year_of_manufacture": year_of_manufacture,
                            "price": price,
                            "fuel": fuel,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    return dataframe


def extract():
    extracted_data = pd.DataFrame(
        columns=["car_model", "year_of_manufacture", "price", "fuel"]
    )

    for csv_file in glob.glob("*.csv"):
        if csv_file != target_file:
            extracted_data = pd.concat(
                [extracted_data, extract_from_csv(csv_file)], ignore_index=True
            )

    for json_file in glob.glob("*.json"):
        extracted_data = pd.concat(
            [extracted_data, extract_from_json(json_file)], ignore_index=True
        )

    for xml_file in glob.glob("*.xml"):
        extracted_data = pd.concat(
            [extracted_data, extract_from_xml(xml_file)], ignore_index=True
        )

    return extracted_data


def transform(data: pd.DataFrame):
    data["price"] = round(data["price"], 2)

    return data


def load_data(target_file, transformed_data: pd.DataFrame):
    transformed_data.to_csv(target_file)


def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + "," + message + "\n")


log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()

log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed data")
print(transformed_data)

log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)

log_progress("Load phase Ended")

log_progress("ETL Job Ended")
