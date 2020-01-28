# app.py

import os
import json
import base64

from flask import Flask, request, jsonify
from bdc_db import BDCDatabase
from config import USER, PASSWORD, HOST, DBNAME

from datastorm.business import CubeBusiness
from datastorm.validators import validate

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{}:{}@{}:5432/{}'.format(
   USER, PASSWORD, HOST, DBNAME
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

BDCDatabase(app)
business = CubeBusiness()

#########################################
# REQUEST HTTP -> from API Gateway
#########################################
@app.route("/", methods=["GET"])
def status():
    return jsonify(
        message = 'Running'
    ), 200


@app.route("/create", methods=["POST"])
def create():
    # validate params
    data, status = validate(request.json, 'create')
    if status is False:
        return jsonify(json.dumps(data)), 400

    cubes, status = business.create_cube(data)
    return jsonify(
        message = 'Cube created',
        cubes = cubes
    ), status


@app.route("/start", methods=["GET"])
def start():
    # validate params
    data, status = validate(request.args.to_dict(), 'process')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.start_process(data)
    return jsonify(message), status


@app.route("/create-grs", methods=["POST"])
def craete_grs():
    # validate params
    data, status = validate(request.json, 'grs')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_grs(**data)
    return jsonify(message), status


@app.route("/create-raster-size", methods=["POST"])
def craete_raster_size():
    # validate params
    data, status = validate(request.json, 'raster_size')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_raster_size(**data)
    return jsonify(message), status


#########################################
# REQUEST -> from SQS trigger or Kinesis 
#########################################
def continue_process(event, context):
    with app.app_context():
        params_list = []
        if 'Records' in event:
            for record in event['Records']:
                if 'kinesis' in record:
                    payload=base64.b64decode(record["kinesis"]["data"])
                    params = json.loads(payload)
                    params_list.append(params)
                else:
                    params = json.loads(record['body'])
                    params_list.append(params)
        else:
            params = event
            params_list.append(params)

        message = business.continue_process_stream(params_list)
        return message