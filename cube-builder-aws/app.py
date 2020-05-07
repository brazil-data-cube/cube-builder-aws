# app.py

try:
    import unzip_requirements
except ImportError:
    pass

import os
import json
import base64

from flask import Flask, request, jsonify
from flask_redoc import Redoc
from bdc_db import BDCDatabase
from config import USER, PASSWORD, HOST, DBNAME

from cube_builder_aws.business import CubeBusiness
from cube_builder_aws.validators import validate
from cube_builder_aws.utils.auth import require_oauth_scopes
from cube_builder_aws.version import __version__ 

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{}:{}@{}:5432/{}'.format(
   USER, PASSWORD, HOST, DBNAME
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['REDOC'] = {
    'title': 'Cube Builder AWS',
    'spec_route': '/docs'
}

BDCDatabase(app)
business = CubeBusiness()
_ = Redoc('./spec/openapi.yaml', app)

#########################################
# REQUEST HTTP -> from API Gateway
#########################################
@app.route("/", methods=["GET"])
def status():
    return jsonify(
        message = 'Running',
        description = 'Cube Builder AWS',
        version = __version__
    ), 200


@app.route("/create", methods=["POST"])
@require_oauth_scopes(scope="cube_builder_aws:metadata:POST")
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
@require_oauth_scopes(scope="cube_builder_aws:process:POST")
def start():
    # validate params
    data, status = validate(request.args.to_dict(), 'process')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.start_process(data)
    return jsonify(message), status


@app.route("/create-grs", methods=["POST"])
@require_oauth_scopes(scope="cube_builder_aws:metadata:POST")
def craete_grs():
    # validate params
    data, status = validate(request.json, 'grs')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_grs(**data)
    return jsonify(message), status


@app.route("/create-raster-size", methods=["POST"])
@require_oauth_scopes(scope="cube_builder_aws:metadata:POST")
def craete_raster_size():
    # validate params
    data, status = validate(request.json, 'raster_size')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_raster_size(**data)
    return jsonify(message), status


@app.route("/cube-status", methods=["GET"])
@require_oauth_scopes(scope="cube_builder_aws:metadata:GET")
def get_status():
    # validate params
    data, status = validate(request.args.to_dict(), 'status')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.get_cube_status(**data)
    return jsonify(message), status


@app.route('/cubes', defaults=dict(cube_id=None), methods=['GET'])
@app.route('/cubes/<cube_id>', methods=['GET'])
def list_cubes(cube_id):
    if cube_id is not None:
        message, status_code = business.get_cube(cube_id)
    else:
        message, status_code = business.list_cubes()

    return jsonify(message), status_code


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