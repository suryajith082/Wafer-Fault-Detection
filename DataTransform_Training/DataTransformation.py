from datetime import datetime
from os import listdir
import pandas
import boto3
import os
from application_logging.logger import App_Logger


class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: Suryajith Nair
               Version: 1.0
               Revisions: None

               """

     def __init__(self):
          self.goodDataPath = "good-raw"
          self.s3client = boto3.client('s3')
          self.s3 = boto3.resource('s3')
          self.logger = App_Logger()


     def replaceMissingWithNull(self):
          """
                                           Method Name: replaceMissingWithNull
                                           Description: This method replaces the missing values in columns with "NULL" to
                                                        store in the table. We are using substring in the first column to
                                                        keep only "Integer" data for ease up the loading.
                                                        This column is anyways going to be removed during training.

                                            Written By: Suryajith Nair
                                           Version: 1.0
                                           Revisions: None

                                                   """

          #log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
          try:
               bucket = self.s3.Bucket('good-raw')
               km = bucket.objects.all()
               files = []
               for i in km:
                    files.append(i.key)
               for file in files:
                    am = bucket.Object(file).get()
                    csv = pandas.read_csv(am["Body"])
                    ''' onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    csv = pandas.read_csv(self.goodDataPath+"/" + file)'''
                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]
                    csv.to_csv(file, index=None, header=True)
                    self.s3.Bucket('good-raw').upload_file(Filename=file, Key=file)
                    if os.path.exists(file):
                         os.remove(file)
                    name = "dataTransformLog"
                    database = "WAFERFAULT-TRAINING-LOGS"
                    self.logger.log(database,name," %s: File Transformed successfully!!" % file)
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
          except Exception as e:
               name = "dataTransformLog"
               database = "WAFERFAULT-TRAINING-LOGS"
               self.logger.log(database,name, "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               #log_file.close()
          #log_file.close()
