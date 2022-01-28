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
    new_dict = {}
    for key in old_dict:
        new_dict[key]["otherArticles"]["articleNine"] = {"checked": old_dict["articleNine"]["checked"],
                                                         "legalities": [{"referenceUrl": old_dict["articleNine"]["referenceUrl"]}]}
        new_dict[key]["otherArticles"]["articleTen"] = {"checked": old_dict["articleTen"]["checked"],
                                                        "legalities": [{"referenceUrl": old_dict["articleTen"]["referenceUrl"]}]}
    return new_dict


records_file = args.outputdirectory + "mongo_records.json"
output_records = args.outputdirectory + "transformed_records.json"

with open(output_records, 'w', encoding="utf-8") as outfile:
    json.dump(transform_records(records_file), outfile, ensure_ascii=False, indent=4)
