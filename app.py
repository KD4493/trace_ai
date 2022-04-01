from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
#import flask_monitoringdashboard as dashboard
import json
from trainingModel import trainModel
from predictFromModel import prediction
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')
# #
app = Flask(__name__)
#dashboard.bind(app)
CORS(app)
#
@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        pred = prediction()

        pred.predictionFromModel()

        return "successful"


    except ValueError:
        return Response("Error occured! %s" %ValueError)
    except KeyError:
        return Response("Error occured! %s" %KeyError)
    except Exception as e:
        return Response("Error occured! %s" %e)

@app.route("/train", methods=['GET'])
@cross_origin()
def trainRouteClient():

    try:
        trainObj = trainModel()
        trainObj.trainingModel()

    except ValueError:
        return Response("Error occured! %s" %ValueError)
    except KeyError:
        return Response("Error occured! %s" %KeyError)
    except Exception as e:
        return Response("Error occured! %s" %e)
    return Response("Training successful!!")

# port = int(os.getenv("PORT",5000))
# if __name__ == "__main__":
#     host = '0.0.0.0'
#     httpd = simple_server.make_server(host, port, app)
#     httpd.server_forever()



#
# def trainroute():
#     try:
#         trainObj = trainModel()   #object initialization
#         trainObj.trainingModel()    #training the model
#         print("Training successful")
# #
# #
#     except Exception as e:
#         print("Data prepartion unsuccessful !!!")
#         #print('Training unsuccessful')
#         raise e
#
# def predictroute():
#     try:
#         #data_prep_pred = dataPrePreparation()
#         #data_prep_pred.dataPrePreparation()
#         pred = prediction()
#
#         pred.predictionFromModel()
#
#     except Exception as e:
#         print('Error in predicting results.Exception message: ', str(e))
#         raise e


#sched = BackgroundScheduler(daemon=True)
# sched.add_job(trainroute,'interval',seconds=180)
# sched.start()


# app = Flask(__name__)
# #dashboard.bind(app)
# CORS(app)
#
# @app.route("/", methods=['GET'])
# @cross_origin()
# def home():
#     return render_template('index.html')

# trainroute()
# predictroute()

# port = int(os.getenv("PORT",5000))
# if __name__ == "__main__":
#     host = '0.0.0.0'
#     #port = 5000
#     httpd = simple_server.make_server(host, port, app)
#     # print("Serving on %s %d" % (host, port))
#     httpd.serve_forever()

port = int(os.getenv("PORT",5001))
if __name__ == "__main__":
    app.run(port=port, debug=True)








