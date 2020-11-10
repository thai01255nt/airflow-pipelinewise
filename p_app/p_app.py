import pymongo, os
from flask import request, Flask
import json, copy
from bson.objectid import ObjectId
from .ultis import run_tap_cli, generate_p_yaml
import threading
from flask_cors import CORS,cross_origin

# Create app
app = Flask(__name__)
CORS(app)
# Connect MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["pipelinewise"]
p_config_db = mydb["p_config"]


# Variable path
class PATH():
    BASEDIR_APP = app.root_path
    MEDIA_DIR = os.path.join(BASEDIR_APP, "media")
    PIPELINEWISE_HOME = os.path.join(os.path.dirname(BASEDIR_APP), "pipelinewise")
    PIPELINEWISE_VENV = os.path.join(PIPELINEWISE_HOME, ".virtualenvs/pipelinewise/bin/activate")


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/api/create', methods=['POST'])
@cross_origin()
def create_p():
    if request.method == 'POST':
        body = json.loads(request.data.decode())
        print(request.values)
        tap_config = copy.deepcopy(body["tap_config"])
        target_config = copy.deepcopy(body["target_config"])
        # Create record on DB
        config_record = {
            "tap_config": tap_config,
            "target_config": target_config,
            "status": "STOPPED"
        }
        p_config_db.insert(config_record)

        # Update tap_id and target_id of record
        p_id = str(config_record["_id"])
        config_record["message"] = "success"
        config_record["tap_config"]["id"] = "tap_config_{}".format(p_id)
        config_record["target_config"]["id"] = "target_config_{}".format(p_id)
        config_record["tap_config"]["target"] = config_record["target_config"]["id"]
        p_config_db.update_one({'_id': config_record["_id"]}, {"$set": config_record}, upsert=False)

        # Create status record on DB

        config_record["_id"] = str(config_record["_id"])
        return config_record, 200


@app.route('/api/list', methods=['GET'])
def get_list():
    if request.method == 'GET':
        cursor = p_config_db.find({})
        data = []
        for document in cursor:
            data.append(copy.deepcopy(document))
            data[-1]["_id"] = str(data[-1]["_id"])
        print(data)
        return {"output": data}, 200


@app.route('/api/detail/<p_id>', methods=['GET'])
def get_detail(p_id):
    if request.method == 'GET':
        cursor = p_config_db.find({"_id": ObjectId(p_id)})
        if cursor.count() == 0:
            return {"message": "pipelinewise id not exists"}, 400
        document = cursor[0]
        data = copy.deepcopy(document)
        data["_id"] = str(data["_id"])
        return {"output": data}, 200


@app.route('/api/execute', methods=['POST'])
def execute():
    if request.method == 'POST':
        body = json.loads(request.data.decode())
        p_id = body["p_id"]
        cursor = p_config_db.find({"_id": ObjectId(p_id)})
        if cursor.count() == 0:
            return {"message": "pipelinewise id not exists"}, 400
        document = cursor[0]
        tap_config = document["tap_config"]
        target_config = document["target_config"]
        config_dir, tap_id, target_id = generate_p_yaml(p_id=p_id, tap_config=tap_config, target_config=target_config)

        run_task_thread = threading.Thread(target=run_tap_cli,
                                           kwargs={"config_dir": config_dir, "p_id": p_id, "tap_id": tap_id,
                                                   "target_id": target_id})
        run_task_thread.start()
        return {"message": "request execute pipelinewise success"}, 200

        # TODO pending config timeout


@app.route('/api/get_status/<p_id>', methods=['GET'])
def get_status(p_id):
    if request.method == 'GET':
        p_config_objects = p_config_db.find({"_id": ObjectId(p_id)})
        if p_config_objects.count() == 0:
            return {"message": "pipelinewise id not exists"}, 400
        document = p_config_objects[0]
        return {"status": document["status"]}, 200

        # TODO pending config timeout
