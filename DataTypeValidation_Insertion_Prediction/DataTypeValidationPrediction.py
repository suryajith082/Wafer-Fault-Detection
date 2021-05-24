import boto3
import pymongo
from datetime import datetime
from os import listdir
import os
import pandas as pd
from application_logging.logger import App_Logger


class dBOperation:
    """
          This class shall be used for handling all the SQL operations.

          Written By: Suryajith Nair
          Version: 1.0
          Revisions: None

          """

    def __init__(self):
        self.badFilePath = "bad-raw"
        self.goodFilePath = "good-raw"
        self.client = pymongo.MongoClient(
            "mongodb+srv://user:acer@validation-schema.v5gw1.mongodb.net/WAFERFAULT-DETECTION?retryWrites=true&w=majority")

        self.s3client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.logger = App_Logger()




    def insertIntoTableGoodData(self,Database):

        """
                                       Method Name: insertIntoTableGoodData
                                       Description: This method inserts the Good data files from the Good_Raw folder into the
                                                    above created table.
                                       Output: None
                                       On Failure: Raise Exception

                                        Written By: Suryajith Nair
                                       Version: 1.0
                                       Revisions: None

                """

        now = datetime.now()
        date = now.date()
        current_time = now.strftime("%H:%M:%S")
        conn = self.client[Database]
        collection = conn["Good_Raw_Data" + "_" + str(date) + "_" + str(current_time)]
        bucket = self.s3.Bucket(self.goodFilePath)
        km = bucket.objects.all()
        files = []
        for i in km:
            files.append(i.key)
        combined_csv = pd.concat([pd.read_csv(bucket.Object(f).get()["Body"]) for f in files], ignore_index=True)
        csv_dict = combined_csv.to_dict('records')
        collection.insert_many(csv_dict)
        return combined_csv

