from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger

class train_validation:
    def __init__(self,path,mailid,password):
        self.raw_data = Raw_Data_validation(path,mailid,password)
        self.dataTransform = dataTransform()
        self.dBOperation = dBOperation()
        #self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()

    def train_validation(self):
        try:
            database="WAFERFAULT-TRAINING-LOGS"
            name="Training_Main_Log"
            self.log_writer.log(database,name, 'Start of Validation on files!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(database,name, "Raw Data Validation Complete!!")

            self.log_writer.log(database,name, "Starting Data Transforamtion!!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.log_writer.log(database,name, "DataTransformation Completed!!!")

            self.log_writer.log(database,name,
                                "Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            # insert csv files in the table
            pandasfile=self.dBOperation.insertIntoTableGoodData('TrainingBase')
            self.log_writer.log(database,name, "Insertion in Database Table completed!!!")
            self.log_writer.log(database,name, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(database,name, "Good_Data folder deleted!!!")
            self.log_writer.log(database,name, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(database,name, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(database,name, "Validation Operation completed!!")
            self.log_writer.log(database,name, "Extracting csv file from table")
            # export data in table to csvfile
            #self.dBOperation.selectingDatafromtableintocsv('Training')
            #self.file_object.close()
            return pandasfile

        except Exception as e:
            raise e









