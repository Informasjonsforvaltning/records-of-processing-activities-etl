import json
import os
from pymongo import MongoClient
import argparse
from bson.jsonutil import dumps

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--outputdirectory', help="the path to the directory of the output files", required=True)
args = parser.parse_args()
connection = MongoClient(
    f"""mongodb://{os.environ['MONGO_USERNAME']}:{os.environ['MONGO_PASSWORD']}@mongodb:27017/records-of-processing-activities?authSource=admin&authMechanism=SCRAM-SHA-1""")

db = connection['records-of-processing-activities']


# Records
dict_list = list(db.records.find())
records = {}
for id_dict in dict_list:
    id_str = id_dict["recordId"]
    records[id_str] = id_dict
print("Total number of extracted records: " + str(len(records)))

with open(args.outputdirectory + 'mongo_records.json', 'w', encoding="utf-8") as outfile:
    records_json_str = dumps(records)
    outfile.write(records_json_str)

# Organizations
dict_list = list(dumps(db.organizations.find()))
organizations = {}
for id_dict in dict_list:
    id_str = id_dict["organizationId"]
    organizations[id_str] = id_dict
print("Total number of extracted organizations: " + str(len(organizations)))

with open(args.outputdirectory + 'mongo_organizations.json', 'w', encoding="utf-8") as outfile:
    organizations_json_str = dumps(organizations)
    outfile.write(organizations_json_str)
