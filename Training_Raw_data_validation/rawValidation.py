from datetime import datetime
import os
import re
import pymongo
import pandas as pd
import boto3
from application_logging.logger import App_Logger
from mail import EmailClass






class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.

             Written By: Suryajith Nair
             Version: 1.0
             Revisions: None

             """

    def __init__(self,path,sender,password):
        self.Batch_Directory = path
        self.client = pymongo.MongoClient("mongodb+srv://user:acer@validation-schema.v5gw1.mongodb.net/WAFERFAULT-DETECTION?retryWrites=true&w=majority")
        self.database = self.client["WAFERFAULT-DETECTION"]
        self.collection = self.database["TRAIN-VALIDATION-SCHEMA"]
        self.result = self.collection.find({})
        self.mainresult=self.result[0]
        #self.schema_path = 'schema_training.json'
        self.logger = App_Logger()
        self.s3client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.sender = sender
        self.password = password
        self.mail = EmailClass()

    def valuesFromSchema(self):
        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception

                         Written By: Suryajith Nair
                        Version: 1.0
                        Revisions: None

                                """
        try:
            '''with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()'''
            pattern = self.mainresult['SampleFileName']
            LengthOfDateStampInFile = self.mainresult['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = self.mainresult['LengthOfTimeStampInFile']
            column_names = self.mainresult['ColName']
            NumberofColumns = self.mainresult['NumberofColumns']
            '''pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')'''
            name="valuesfromSchemaValidationLog"
            database="WAFERFAULT-TRAINING-LOGS"
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(database,name,message)

            #file.close()



        except ValueError:
            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            name="valuesfromSchemaValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name,"ValueError:Value not found inside schema_training.json")
            #file.close()
            raise ValueError

        except KeyError:
            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            name="valuesfromSchemaValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name, "KeyError:Key value error incorrect key passed")
           #file.close()
            raise KeyError

        except Exception as e:
            #file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            name="valuesfromSchemaValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name, str(e))
            #file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                 Written By: Suryajith Nair
                                Version: 1.0
                                Revisions: None

                                        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError

                                       Written By: Suryajith Nair
                                      Version: 1.0
                                      Revisions: None

                                              """

        try:
            '''path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)'''
            j=[]
            for bucket in self.s3.buckets.all():
                j.append(bucket.name)
            if "good-raw" not in j:
                self.s3.create_bucket(Bucket="good-raw")
            if "bad-raw" not in j:
                self.s3.create_bucket(Bucket="bad-raw")

        except Exception as e:
            #file = open("Training_Logs/GeneralLog.txt", 'a+')
            name="GeneralLog"
            database="WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name,"Error while creating Directory %s:" % e)
            #file.close()
            raise e

    def deleteExistingGoodDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made  to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: Suryajith Nair
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            '''path = 'Training_Raw_files_validated/''
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                name = "GeneralLog"
                #file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(name,"GoodRaw directory deleted successfully!!!")
                #file.close()'''
            m = []
            for bucket in self.s3.buckets.all():
                m.append(bucket.name)
            if "good-raw" in m:
                s3bucket = self.s3.Bucket('good-raw')
                s3bucket.objects.all().delete()
                self.s3client.delete_bucket(Bucket="good-raw")
                name = "GeneralLog"
                database="WAFERFAULT-TRAINING-LOGS"
                self.logger.log(database,name, "GoodRaw directory deleted successfully!!!")
        except Exception as s:
            name = "GeneralLog"
            #file = open("Training_Logs/GeneralLog.txt", 'a+')
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name,"Error while Deleting Directory : %s" %s)
            #file.close()
            raise s

    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: Suryajith Nair
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            '''path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                name = "GeneralLog"
                #file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(name,"BadRaw directory deleted before starting validation!!!")
                #file.close()'''
            m=[]
            for bucket in self.s3.buckets.all():
                m.append(bucket.name)
            if "bad-raw" in m:
                s3bucket = self.s3.Bucket('bad-raw')
                s3bucket.objects.all().delete()
                self.s3client.delete_bucket(Bucket="bad-raw")
                name = "GeneralLog"
                database = "WAFERFAULT-TRAINING-LOGS"
                self.logger.log(database,name, "BadRaw directory deleted before starting validation!!!")

        except Exception as s:
            name = "GeneralLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            #file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(database,name,"Error while Deleting Directory : %s" %s)
            #file.close()
            raise s

    def moveBadFilesToArchiveBad(self):

        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError

                                             Written By: Suryajith Nair
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            '''source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()'''
            dictionary=[]
            for bucket in self.s3.buckets.all():
                dictionary.append(bucket.name)
            if "bad-raw" in dictionary:
                if "trainingarchivebaddata" not in dictionary:
                    self.s3.create_bucket(Bucket="trainingarchivebaddata")
                dest = 'BadData_' + str(date) + "_" + str(time)
                self.s3client.put_object(Bucket="trainingarchivebaddata", Key=(dest)+'/')
                badraw = self.s3.Bucket('bad-raw')
                km = badraw.objects.all()
                D = []
                for i in km:
                    D.append(i.key)
                self.mail.email(D, self.sender, self.password)
                for i in D:
                    copy_source = {
                        'Bucket': 'training-batch-file',
                        'Key': i
                    }
                    self.s3.meta.client.copy(copy_source, 'trainingarchivebaddata', dest + '/' + i)
                name = "GeneralLog"
                database = "WAFERFAULT-TRAINING-LOGS"
                self.logger.log(database,name, "Bad files moved to archive")
                self.deleteExistingBadDataTrainingFolder()
                self.logger.log(database, name, "Bad Raw Data Folder Deleted successfully!!")


        except Exception as e:
            #file = open("Training_Logs/GeneralLog.txt", 'a+')
            name = "GeneralLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name, "Error while moving bad files to archive:: %s" % e)
            #file.close()
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: Suryajith Nair
                    Version: 1.0
                    Revisions: None

                """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        #onlyfiles = [f for f in listdir(self.Batch_Directory)]
        bucket = self.s3.Bucket(self.Batch_Directory)
        km = bucket.objects.all()
        filenames = []
        for i in km:
            filenames.append(i.key)
        try:
            #f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in filenames:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            copy_source = {
                                'Bucket': 'training-batch-file',
                                'Key': filename
                            }
                            self.s3.meta.client.copy(copy_source, 'good-raw', filename)
                            name = "nameValidationLog"
                            database = "WAFERFAULT-TRAINING-LOGS"
                            self.logger.log(database,name,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            copy_source = {
                                'Bucket': 'training-batch-file',
                                'Key': filename
                            }
                            self.s3.meta.client.copy(copy_source, 'bad-raw', filename)
                            self.s3.Object('good-raw', filename).delete()
                            name = "nameValidationLog"
                            database = "WAFERFAULT-TRAINING-LOGS"
                            self.logger.log(database,name,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        copy_source = {
                            'Bucket': 'training-batch-file',
                            'Key': filename
                        }
                        self.s3.meta.client.copy(copy_source, 'bad-raw', filename)
                        self.s3.Object('good-raw', filename).delete()
                        name = "nameValidationLog"
                        database = "WAFERFAULT-TRAINING-LOGS"
                        self.logger.log(database,name,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    #shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    copy_source = {
                        'Bucket': 'training-batch-file',
                        'Key': filename
                    }
                    self.s3.meta.client.copy(copy_source, 'bad-raw', filename)
                    self.s3.Object('good-raw', filename).delete()
                    name="nameValidationLog"
                    database="WAFERFAULT-TRAINING-LOGS"
                    self.logger.log(database,name, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            #f.close()

        except Exception as e:
            name = "nameValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            #f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(database,name, "Error occured while validating FileName %s" % e)
            #f.close()
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

                           Written By: Suryajith Nair
                          Version: 1.0
                          Revisions: None

                      """
        try:
            #f = open("Training_Logs/columnValidationLog.txt", 'a+')
            name = "ColumnValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name,"Column Length Validation Started!!")
            bucket = self.s3.Bucket('good-raw')
            km = bucket.objects.all()
            files = []
            for i in km:
                files.append(i.key)
            for file in files:
                am = bucket.Object(file).get()
                csv=pd.read_csv(am["Body"])
                #csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    #shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    copy_source = {
                        'Bucket': 'training-batch-file',
                        'Key': file
                    }
                    self.s3.meta.client.copy(copy_source, 'bad-raw', file)
                    self.s3.Object('good-raw', file).delete()
                    self.logger.log(database,name, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(database,name, "Column Length Validation Completed!!")
        except OSError:
            #f = open("Training_Logs/columnValidationLog.txt", 'a+')
            name = "ColumnValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name, "Error Occured while moving the file :: %s" % OSError)
            #f.close()
            raise OSError
        except Exception as e:
            #f = open("Training_Logs/columnValidationLog.txt", 'a+')
            name = "ColumnValidationLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name, "Error Occured:: %s" % e)
            #f.close()
            raise e
        #f.close()

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: Suryajith Nair
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            #f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            name = "missingValuesInColumnLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name,"Missing Values Validation Started!!")
            bucket = self.s3.Bucket('good-raw')
            km = bucket.objects.all()
            files = []
            for i in km:
                files.append(i.key)
            for file in files:
                am = bucket.Object(file).get()
                csv = pd.read_csv(am["Body"])
                #csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        #shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                        #            "Training_Raw_files_validated/Bad_Raw")
                        copy_source = {
                            'Bucket': 'training-batch-file',
                            'Key': file
                        }
                        self.s3.meta.client.copy(copy_source, 'bad-raw', file)
                        self.s3.Object('good-raw', file).delete()
                        self.logger.log(database,name,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv(file, index=None, header=True)
                    self.s3.Bucket('good-raw').upload_file(Filename=file, Key=file)
                    if os.path.exists(file):
                        os.remove(file)
        except OSError:
            #f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            name = "missingValuesInColumnLog"
            database = "WAFERFAULT-TRAINING-LOGS"
            self.logger.log(database,name, "Error Occured while moving the file :: %s" % OSError)
            #f.close()
            raise OSError
        except Exception as e:
           # f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
           name = "missingValuesInColumnLog"
           database = "WAFERFAULT-TRAINING-LOGS"
           self.logger.log(database,name, "Error Occured:: %s" % e)
            #f.close()
           raise e
        #f.close()












