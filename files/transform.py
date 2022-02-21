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
    return transformed_records


def openfile(file_name):
    with open(file_name) as json_file:
        return json.load(json_file)


def transform_record(old_dict):
    new_dict = old_dict
    for key, value in old_dict.items():
        if check_content(value) and old_dict["commonDataControllerContact"]["commonDataControllerChecked"] is not False:
            new_dict["commonDataControllerContact"]["commonDataControllerChecked"] = True
        else:
            new_dict["commonDataControllerContact"]["commonDataControllerChecked"] = None
    return new_dict


def check_content(content):
    if isinstance(content, str) and len(content) > 0:
        return True
    elif isinstance(content, list):
        for string in content:
            if len(string) > 0:
                return True
    else:
        return False


records_file = args.outputdirectory + "mongo_records.json"
output_records = args.outputdirectory + "transformed_records.json"

with open(output_records, 'w', encoding="utf-8") as outfile:
    json.dump(transform_records(records_file), outfile, ensure_ascii=False, indent=4)
