import os
import sys
import json
import pandas as pd
import numpy as np
import pymongo
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from dotenv import load_dotenv

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi

ca = certifi.where()


class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_converter(self, file_path):
        """
                data.T.to_json():

        data.T transposes the DataFrame (rows become columns and vice versa).

        .to_json() converts the transposed DataFrame to a JSON string in dictionary format.

        json.loads(...):

        Parses the JSON string into a Python dictionary.

        .values():

        Gets all the dictionary values — which are the individual records (rows) from the CSV.

        list(...):

        Converts those values into a list of dictionaries (records)."""
        try:
            data = pd.read_csv("Network_Data/phisingData.csv")
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        '''
        Inserts a list of records into a MongoDB collection.

        Steps:
        1. Store input parameters
        2. Create MongoDB client using secure URI
        3. Access the target database and collection
        4. Insert all records using insert_many
        5. Return the count of inserted records
        '''
        try:
            self.database = database
            self.collection = collection
            self.records = records
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]

            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return len(self.records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        


if __name__=='__main__':
    FILE_PATH="Network_Data\phisingData.csv"
    DATABASE="AdityaAI"
    Collection="NetworkData"
    networkobj=NetworkDataExtract()
    records=networkobj.csv_to_json_converter(file_path=FILE_PATH)
    print(records)
    no_of_records=networkobj.insert_data_mongodb(records,DATABASE,Collection)
    print(no_of_records)



# Loads required libraries and environment variables
# Connects securely to MongoDB Atlas using URI from .env
# Defines NetworkDataExtract class to handle data ingestion

# Method: csv_to_json_converter
# - Reads a CSV file
# - Resets index
# - Converts the DataFrame into a list of JSON-like dictionaries (records)

# Method: insert_data_mongodb
# - Connects to MongoDB using pymongo and certifi (for SSL)
# - Selects the target database and collection
# - Inserts all records using insert_many
# - Returns the count of inserted records

# Main block:
# - Instantiates NetworkDataExtract
# - Converts CSV to JSON records
# - Inserts them into MongoDB
# - Prints sample records and insertion count
