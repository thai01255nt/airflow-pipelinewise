import pymongo, os
from flask import request, Flask
import json, copy, threading
from bson.objectid import ObjectId
from . import p_connect
import threading
from flask_cors import CORS,cross_origin
# Create app
app = Flask(__name__)
CORS(app)
# Connect MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["airflow"]
dag_config_db = mydb["dag_config"]


# Variable path
class PATH():
    BASEDIR_APP = app.root_path
    MEDIA_DIR = os.path.join(BASEDIR_APP, "media")
    PIPELINEWISE_HOME = os.path.join(os.path.dirname(BASEDIR_APP), "pipelinewise")
    PIPELINEWISE_VENV = os.path.join(PIPELINEWISE_HOME, ".virtualenvs/pipelinewise/bin/activate")


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/api/transfer/create', methods=['POST'])
@cross_origin()
def create_transfer_dag():
    if request.method == 'POST':
        body = json.loads(request.data.decode())
        p_config = copy.deepcopy(body["p_config"])
        dag_config = copy.deepcopy(body["dag_config"])

        # Create record on DB
        config_record = {
            "dag_config": dag_config,
            "p_config": p_config,
            "is_complete": 0,
            "status": "PAUSED",
            "pre_interval_status": "",
            "last_time_interval": ""
        }
        dag_config_db.insert(config_record)

        # Create pipelinewise and update record
        p_config = p_connect.create_p(p_config)
        p_config["p_id"] = p_config["_id"]
        del p_config["status"]
        del p_config["message"]
        del p_config["_id"]
        dag_id = str(config_record["_id"])
        config_record["dag_config"]["dag_id"] = dag_id
        config_record["p_config"] = p_config
        dag_config_db.update_one({'_id': config_record["_id"]}, {"$set": config_record}, upsert=False)

        # Create status record on DB

        config_record["_id"] = str(config_record["_id"])
        return config_record, 200


# TODO dag_args, auto generate as follow dag_type or customn

@app.route('/api/list', methods=['GET'])
def get_list():
    if request.method == 'GET':
        cursor = dag_config_db.find({})
        data = []
        for document in cursor:
            data.append(copy.deepcopy(document))
            data[-1]["_id"] = str(data[-1]["_id"])
        print(data)
        return {"output": data}, 200


@app.route('/api/detail/<dag_id>', methods=['GET'])
def get_detail(dag_id):
    if request.method == 'GET':
        cursor = dag_config_db.find({"_id": ObjectId(dag_id)})
        if cursor.count() == 0:
            return {"message": "dag id not exists"}, 400
        document = cursor[0]
        data = copy.deepcopy(document)
        data["_id"] = str(data["_id"])
        return {"output": data}, 200


@app.route('/api/get_status/<dag_id>', methods=['GET'])
def get_status(dag_id):
    if request.method == 'GET':
        dag_config_objects = dag_config_db.find({"_id": ObjectId(dag_id)})
        if dag_config_objects.count() == 0:
            return {"message": "dag id not exists"}, 400
        document = dag_config_objects[0]
        return {"status": document["status"]}, 200

        # TODO pending config timeout
