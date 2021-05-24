import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
import pandas
from application_logging import logger
import os
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
import boto3
from datetime import datetime


class prediction:

    def __init__(self):
        #self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.database="WAFERFAULT-PREDICTION-LOGS"
        self.name="Prediction_from_Model_Log"
        self.log_writer = logger.App_Logger()
        self.s3client = boto3.client('s3')
        self.s3 = boto3.resource('s3')

    def predictionFromModel(self,dataframe):

        try:
            #self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.log_writer.log(self.database,self.name,'Start of Prediction')
            '''data_getter=data_loader_prediction.Data_Getter_Pred(self.database,self.name,self.log_writer)
            data=data_getter.get_data()'''
            data=dataframe
            preprocessor=preprocessing.Preprocessor(self.database,self.name,self.log_writer)
            is_null_present=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(data)
            data=preprocessor.remove_columns(data,cols_to_drop)
            #data=data.to_numpy()
            file_loader=file_methods.File_Operation(self.database,self.name,self.log_writer)
            kmeans=file_loader.load_model('KMeans')

            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(data.drop(['Wafer'],axis=1))#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            listofoutput=[]
            for i in clusters:
                cluster_data= data[data['clusters']==i]
                wafer_names = list(cluster_data['Wafer'])
                cluster_data=data.drop(labels=['Wafer'],axis=1)
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result=list(model.predict(cluster_data))
                listofoutput=list(zip(wafer_names,result))
            now = datetime.now()
            date = now.date()
            time = now.strftime("%H%M%S")
            result = pandas.DataFrame(listofoutput,columns=['Wafer','Prediction'])

            result = result.sort_values(by='Wafer', ascending=True)
            filename="Prediction"+str(date)+"_"+str(time)+".csv"
            result.to_csv(filename,header=True,mode='a+',index=False)#appends result to prediction file
            self.s3.Bucket('final-prediction-output').upload_file(Filename=filename, Key=filename)
            if os.path.exists(filename):
                os.remove(filename)
            path = "final-prediction-output/"+filename
            self.log_writer.log(self.database,self.name,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.database,self.name, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path, result.head().to_json(orient="records")




