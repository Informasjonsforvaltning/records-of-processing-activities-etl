import os
from pymongo import MongoClient
import argparse
from bson.json_util import dumps

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--outputdirectory', help="the path to the directory of the output files", required=True)
args = parser.parse_args()
connection = MongoClient(
    f"""mongodb://{os.environ['MONGO_USERNAME']}:{os.environ['MONGO_PASSWORD']}@mongodb:27017/recordsDB?authSource=admin&authMechanism=SCRAM-SHA-1""")

db = connection['recordsDB']


records_list = list(db.records.find())
records = {}
for id_dict in records_list:
    _id = id_dict["recordId"]
    records[_id] = {}
    records[_id]["commonDataControllerContact"] = id_dict["commonDataControllerContact"]

print("Total number of extracted records: " + str(len(records)))

with open(args.outputdirectory + 'mongo_records.json', 'w', encoding="utf-8") as outfile:
    records_json_str = dumps(records)
    outfile.write(records_json_str)

