from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
import flask_monitoringdashboard as dashboard
from predictFromModel import prediction
import json
from os import listdir
import pandas as pd

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/traininpage", methods=['GET'])
@cross_origin()
def trainingpage():
    return render_template('training.html')

@app.route("/contactpage", methods=['GET'])
@cross_origin()
def contactpage():
    return render_template('contactpage.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:

        if request.json is not None:
            path = request.json['filepath']
            sender=request.json['mailid']
            password=request.json["password"]
            pred_val = pred_validation(path,sender,password) #object initialization

            pandasFileAfterTraining=pred_val.prediction_validation() #calling the prediction_validation function'''
            '''onlyfiles = [f for f in listdir("C:\\Users\\Acer\\Downloads\\Good_Raw2")]
            d = []
            for f in onlyfiles:
                d.append(pd.read_csv("C:\\Users\\Acer\\Downloads\\Good_Raw2\\" + f))
            pandasFileAfterTraining = pd.concat(d, ignore_index=True)'''

            pred = prediction() #object initialization
            # predicting for dataset present in database
            path,json_predictions = pred.predictionFromModel(pandasFileAfterTraining)
            return Response("Prediction File created at !!!"  +str(path) +'and few of the predictions are '+str(json.loads(json_predictions)))
        else:
            print('Nothing Matched')
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)



@app.route("/train", methods=['GET','POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath'].lower()
            sender = request.json['mailid']
            password = request.json["password"]
            train_valObj = train_validation(path,sender,password) #object initialization

            pandasFileAfterTraining=train_valObj.train_validation()#calling the training_validation function
            '''onlyfiles = [f for f in listdir("C:\\Users\\Acer\\Downloads\\Good_Raw")]
            d = []
            for f in onlyfiles:
                d.append(pd.read_csv("C:\\Users\\Acer\\Downloads\\Good_Raw\\" + f))
            pandasFileAfterTraining = pd.concat(d,ignore_index=True)'''
            trainModelObj = trainModel() #object initialization
            trainModelObj.trainingModel(pandasFileAfterTraining) #training the model for the files in the table


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

port = int(os.getenv("PORT",8000))
if __name__ == "__main__":
    host = '0.0.0.0'
    #port = 8000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
