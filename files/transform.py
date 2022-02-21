import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('-o', '--outputdirectory', help="the path to the directory of the output files", required=True)
args = parser.parse_args()


def transform_records(r_file):
    records = openfile(r_file)
    transformed_records = {}
    print("Total number of extracted records: " + str(len(records)))
    for record_id in records:
        transformed_records[record_id] = transform_record(records[record_id])
    print("Total number of transformed records: " + str(len(records)))
    return transformed_records


def openfile(file_name):
    with open(file_name) as json_file:
        return json.load(json_file)


def transform_record(old_dict):
    new_dict = {}
    for key, value in old_dict.items():
        checked = old_dict["commonDataControllerContact"].get("commonDataControllerChecked")
        if checked and isinstance(checked, bool):
            new_dict["commonDataControllerContact"]["commonDataControllerChecked"] = checked
        elif check_content(value):
            new_dict["commonDataControllerContact"]["commonDataControllerChecked"] = True
        else:
            new_dict["commonDataControllerContact"]["commonDataControllerChecked"] = None
    new_dict["commonDataControllerContact"]["companies"] = old_dict["commonDataControllerContact"].get("companies")
    new_dict["commonDataControllerContact"]["distributionOfResponsibilities"] = old_dict["commonDataControllerContact"].get("distributionOfResponsibilities")
    new_dict["commonDataControllerContact"]["contactPoints"] = old_dict["commonDataControllerContact"].get("contactPoints")
    return new_dict


def check_content(content):
    if isinstance(content, str) and len(content) > 0:
        return True
    elif isinstance(content, list):
        for obj in content:
            for key in obj:
                if obj.get(key) and len(obj.get(key)) > 0:
                    return True
    return False


records_file = args.outputdirectory + "mongo_records.json"
output_records = args.outputdirectory + "transformed_records.json"

with open(output_records, 'w', encoding="utf-8") as outfile:
    json.dump(transform_records(records_file), outfile, ensure_ascii=False, indent=4)
