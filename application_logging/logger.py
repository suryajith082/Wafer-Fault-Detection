from datetime import datetime
import pymongo

class App_Logger:
    def __init__(self):
        pass

    '''def log(self, file_object, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        file_object.write(
            str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message +"\n")
    '''

    def log(self,database_name, collection_name, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        fileobject={"Date":str(self.date),
                    "Current_Time":str(self.current_time),
                    "Log_Message":str(log_message)}
        url = "mongodb+srv://user:acer@validation-schema.v5gw1.mongodb.net/WAFERFAULT-TRAINING-LOGS?retryWrites=true&w=majority"
        client=pymongo.MongoClient(url)
        database = client[database_name]
        collection = database[collection_name]
        collection.insert_one(fileobject)
