import json
import os
from pymongo import MongoClient
import argparse
from bson.json_util import loads

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--outputdirectory', help="the path to the directory of the output files", required=True)
args = parser.parse_args()
connection = MongoClient(
    f"""mongodb://{os.environ['MONGO_USERNAME']}:{os.environ['MONGO_PASSWORD']}@mongodb:27017/recordsDB?authSource=admin&authMechanism=SCRAM-SHA-1""")
db = connection['recordsDB']

with open(args.outputdirectory + 'transformed_records.json') as records_file:
    records_file_str = records_file.read()
    transformed_json = loads(records_file_str)

    total_updated = 0
    total_failed = 0
    fail_log = {}
    for mongo_id in transformed_json:
        to_be_updated = transformed_json[mongo_id]
        print("Updating ID: " + mongo_id)
        insert_result = db.records.find_one_and_update({'_id': mongo_id}, {'$set': to_be_updated})
        if insert_result:
            total_updated += 1
            print("Successfully updated: " + mongo_id)
        else:
            total_failed += 1
            print("Update failed: " + mongo_id)
            fail_log[mongo_id] = mongo_id
    print("Total number of records updated: " + str(total_updated))
    print("Total number of record updates failed: " + str(total_failed))
    with open(args.outputdirectory + "load_errors.json", 'w', encoding="utf-8") as err_file:
        json.dump(fail_log, err_file, ensure_ascii=False, indent=4)
