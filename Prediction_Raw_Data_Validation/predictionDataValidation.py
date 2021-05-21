from datetime import datetime
import os
import re
import pandas as pd
from application_logging.logger import App_Logger
import pymongo
import boto3





class Prediction_Data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

    def __init__(self,path):
        self.Batch_Directory = path
        self.client = pymongo.MongoClient("mongodb+srv://user:acer@validation-schema.v5gw1.mongodb.net/WAFERFAULT-DETECTION?retryWrites=true&w=majority")
        #self.schema_path = 'schema_prediction.json'
        self.database = self.client["WAFERFAULT-DETECTION"]
        self.collection = self.database["PREDICT-VALIDATION-SCHEMA"]
        self.result = self.collection.find({})
        self.mainresult = self.result[0]
        self.s3client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.logger = App_Logger()


    def valuesFromSchema(self):
        """
                                Method Name: valuesFromSchema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception

                                 Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                                        """
        try:

            pattern = self.mainresult['SampleFileName']
            LengthOfDateStampInFile = self.mainresult['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = self.mainresult['LengthOfTimeStampInFile']
            column_names = self.mainresult['ColName']
            NumberofColumns = self.mainresult['NumberofColumns']
            name = "valuesfromSchemaValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(database, name, message)



        except ValueError:
            #file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            name = "valuesfromSchemaValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database, name,"ValueError:Value not found inside schema_training.json")
            #file.close()
            raise ValueError

        except KeyError:
            name = "valuesfromSchemaValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            #file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(database, name, "KeyError:Key value error incorrect key passed")
            #file.close()
            raise KeyError

        except Exception as e:
            name = "valuesfromSchemaValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            #file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(database, name, str(e))
            #file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        """
                                      Method Name: manualRegexCreation
                                      Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                  This Regex is used to validate the filename of the prediction data.
                                      Output: Regex pattern
                                      On Failure: None

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                        Method Name: createDirectoryForGoodBadRawData
                                        Description: This method creates directories to store the Good Data and Bad Data
                                                      after validating the prediction data.

                                        Output: None
                                        On Failure: OSError

                                         Written By: iNeuron Intelligence
                                        Version: 1.0
                                        Revisions: None

                                                """
        try:
            j = []
            for bucket in self.s3.buckets.all():
                j.append(bucket.name)
            if "good-raw" not in j:
                self.s3.create_bucket(Bucket="good-raw")
            if "bad-raw" not in j:
                self.s3.create_bucket(Bucket="bad-raw")
            name = "GeneralLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database, name, "Created good and bad directory")

        except OSError as ex:
            name = "GeneralLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name,"Error while creating Directory %s:" % ex)
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """
        try:
            m = []
            for bucket in self.s3.buckets.all():
                m.append(bucket.name)
            if "good-raw" in m:
                s3bucket = self.s3.Bucket('good-raw')
                s3bucket.objects.all().delete()
                self.s3client.delete_bucket(Bucket="good-raw")
                name = "GeneralLog"
                database = "WAFERFAULT-PREDICTION-LOGS"
                self.logger.log(database, name, "GoodRaw directory deleted successfully!!!")
        except Exception as s:
            name = "GeneralLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name,"Error while Deleting Directory : %s" %s)
            raise s
    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            m = []
            for bucket in self.s3.buckets.all():
                m.append(bucket.name)
            if "bad-raw" in m:
                s3bucket = self.s3.Bucket('bad-raw')
                s3bucket.objects.all().delete()
                self.s3client.delete_bucket(Bucket="bad-raw")
                name = "GeneralLog"
                database = "WAFERFAULT-PREDICTION-LOGS"
                self.logger.log(database, name, "BadRaw directory deleted before starting validation!!!")

        except Exception as s:
            name = "GeneralLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name,"Error while Deleting Directory : %s" %s)
            raise Exception

    def moveBadFilesToArchiveBad(self):


        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            dictionary = []
            for bucket in self.s3.buckets.all():
                dictionary.append(bucket.name)
            if "bad-raw" in dictionary:
                if "predictionarchivebaddata" not in dictionary:
                    self.s3.create_bucket(Bucket="predictionarchivebaddata")
                dest = 'BadData_' + str(date) + "_" + str(time)
                self.s3client.put_object(Bucket="predictionarchivebaddata", Key=(dest) + '/')
                badraw = self.s3.Bucket('bad-raw')
                km = badraw.objects.all()
                D = []
                for i in km:
                    D.append(i.key)
                for i in D:
                    copy_source = {
                        'Bucket': 'prediction-batch-file',
                        'Key': i
                    }
                    self.s3.meta.client.copy(copy_source, 'predictionarchivebaddata', dest + '/' + i)
                name = "GeneralLog"
                database = "WAFERFAULT-PREDICTION-LOGS"
                self.logger.log(database, name, "Bad files moved to archive")
                self.deleteExistingBadDataTrainingFolder()
                self.logger.log(database, name, "Bad Raw Data Folder Deleted successfully!!")

        except Exception as e:
            name = "GeneralLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name, "Error while moving bad files to archive:: %s" % e)
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the prediction csv file as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

             Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None

        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        bucket = self.s3.Bucket(self.Batch_Directory)
        km = bucket.objects.all()
        filenames = []
        for i in km:
            filenames.append(i.key)

        try:

            for filename in filenames:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            copy_source = {
                                'Bucket': 'prediction-batch-file',
                                'Key': filename
                            }
                            self.s3.meta.client.copy(copy_source, 'good-raw', filename)
                            name = "nameValidationLog"
                            database = "WAFERFAULT-PREDICTION-LOGS"
                            self.logger.log(database, name,
                                            "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            copy_source = {
                                'Bucket': 'prediction-batch-file',
                                'Key': filename
                            }
                            self.s3.meta.client.copy(copy_source, 'bad-raw', filename)
                            self.s3.Object('good-raw', filename).delete()
                            name = "nameValidationLog"
                            database = "WAFERFAULT-PREDICTION-LOGS"
                            self.logger.log(database, name,
                                            "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        copy_source = {
                            'Bucket': 'prediction-batch-file',
                            'Key': filename
                        }
                        self.s3.meta.client.copy(copy_source, 'bad-raw', filename)
                        self.s3.Object('good-raw', filename).delete()
                        name = "nameValidationLog"
                        database = "WAFERFAULT-PREDICTION-LOGS"
                        self.logger.log(database, name,
                                        "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    copy_source = {
                        'Bucket': 'prediction-batch-file',
                        'Key': filename
                    }
                    self.s3.meta.client.copy(copy_source, 'bad-raw', filename)
                    self.s3.Object('good-raw', filename).delete()
                    name = "nameValidationLog"
                    database = "WAFERFAULT-PREDICTION-LOGS"
                    self.logger.log(database, name, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)



        except Exception as e:
            name = "nameValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name, "Error occured while validating FileName %s" % e)
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
                    Method Name: validateColumnLength
                    Description: This function validates the number of columns in the csv files.
                                 It is should be same as given in the schema file.
                                 If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                 If the column number matches, file is kept in Good Raw Data for processing.
                                The csv file is missing the first column name, this function changes the missing name to "Wafer".
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

             """
        try:

            name = "ColumnValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database, name, "Column Length Validation Started!!")
            bucket = self.s3.Bucket('good-raw')
            km = bucket.objects.all()
            files = []
            for i in km:
                files.append(i.key)
            for file in files:
                am = bucket.Object(file).get()
                csv = pd.read_csv(am["Body"])
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    copy_source = {
                        'Bucket': 'prediction-batch-file',
                        'Key': file
                    }
                    self.s3.meta.client.copy(copy_source, 'bad-raw', file)
                    self.s3.Object('good-raw', file).delete()
                    self.logger.log(database, name,
                                    "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(database, name, "Column Length Validation Completed!!")
        except OSError:
            name = "ColumnValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database, name, "Error Occured while moving the file :: %s" % OSError)
            raise OSError
        except Exception as e:
            name = "ColumnValidationLog"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database, name, "Error Occured:: %s" % e)
            #f.close()
            raise e


    '''def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')'''

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            name = "missingValuesInColumn"
            database = "WAFERFAULT-PREDICTION-LOGS"
            #f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(database,name, "Missing Values Validation Started!!")

            bucket = self.s3.Bucket('good-raw')
            km = bucket.objects.all()
            files = []
            for i in km:
                files.append(i.key)
            for file in files:
                am = bucket.Object(file).get()
                csv = pd.read_csv(am["Body"])
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        copy_source = {
                            'Bucket': 'prediction-batch-file',
                            'Key': file
                        }
                        self.s3.meta.client.copy(copy_source, 'bad-raw', file)
                        self.s3.Object('good-raw', file).delete()
                        self.logger.log(database, name,
                                        "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv(file, index=None, header=True)
                    self.s3.Bucket('good-raw').upload_file(Filename=file, Key=file)
                    if os.path.exists(file):
                        os.remove(file)

        except OSError:
            name = "missingValuesInColumn"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name, "Error Occured while moving the file :: %s" % OSError)
            raise OSError
        except Exception as e:
            name = "missingValuesInColumn"
            database = "WAFERFAULT-PREDICTION-LOGS"
            self.logger.log(database,name, "Error Occured:: %s" % e)
            raise e













