#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

try:
    import unzip_requirements
except ImportError:
    pass

from datetime import date
import json
import base64

from flask import Flask, request, jsonify
from flask_redoc import Redoc
from flask_cors import CORS
from bdc_catalog import BDCCatalog
from werkzeug.exceptions import HTTPException

from config import USER, PASSWORD, HOST, DBNAME, PORT
from cube_builder_aws.business import CubeBusiness
from cube_builder_aws.validators import validate
from cube_builder_aws.version import __version__
from cube_builder_aws.logger import logger

class ImprovedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        if isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        return super(ImprovedJSONEncoder, self).default(o)


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{}:{}@{}:{}/{}'.format(
   USER, PASSWORD, HOST, PORT,  DBNAME
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['REDOC'] = {
    'title': 'Cube Builder AWS',
    'spec_route': '/docs'
}
app.json_encoder = ImprovedJSONEncoder
CORS(app)

BDCCatalog(app)
business = CubeBusiness()
_ = Redoc('./spec/openapi.yaml', app)


@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    logger.error(str(e), exc_info=True)
    response = e.get_response()
    # Set Error description as body data.
    response.data = json.dumps(e.description)
    response.content_type = "application/json"
    return response


@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    logger.error(str(e), exc_info=True)

    return jsonify(str(e)), 500


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


@app.route("/create-grs", methods=["POST"])
def craete_grs():
    # validate params
    data, status = validate(request.json, 'grs')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_grs(**data)
    return jsonify(message), status


@app.route('/composite-functions', methods=['GET'])
def list_composite_functions():
    message, status_code = business.list_composite_functions()

    return jsonify(message), status_code


@app.route("/create-cube", methods=["POST"])
def create():
    # validate params
    data, status = validate(request.json, 'create')
    if status is False:
        return jsonify(data), 400

    cubes, status = business.create_cube(data)
    return jsonify(
        message = 'Cube created',
        cubes = cubes
    ), status


@app.route("/start-cube", methods=["POST"])
def start():
    # validate params
    data, status = validate(request.json, 'process')
    if status is False:
        return jsonify(json.dumps(data)), 400

    business = CubeBusiness(url_stac=data['url_stac'], bucket=data['bucket'])
    message, status = business.start_process(data)
    return jsonify(message), status


# @app.route("/cube-status", methods=["GET"])
# def get_status():
#     # validate params
#     data, status = validate(request.args.to_dict(), 'status')
#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status = business.get_cube_status(**data)
#     return jsonify(message), status


# @app.route('/cubes', defaults=dict(cube_id=None), methods=['GET'])
# @app.route('/cubes/<cube_id>', methods=['GET'])
# def list_cubes(cube_id):
#     if cube_id is not None:
#         message, status_code = business.get_cube(cube_id)
#     else:
#         message, status_code = business.list_cubes()

#     return jsonify(message), status_code


# @app.route('/cubes/<cube_id>/tiles', methods=['GET'])
# def list_tiles_as_features(cube_id):
#     message, status_code = business.list_tiles_cube(cube_id)

#     return jsonify(message), status_code


# @app.route('/grs', defaults=dict(grs_id=None), methods=['GET'])
# @app.route('/grs/<grs_id>', methods=['GET'])
# def list_grs_schemas(grs_id):
#     if grs_id is not None:
#         message, status_code = business.get_grs_schema(grs_id)
#     else:
#         message, status_code = business.list_grs_schemas()

#     return jsonify(message), status_code


# @app.route('/list-merges', methods=['GET'])
# def list_merges():
#     data, status = validate(request.args.to_dict(), 'list_merge_form')
#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status_code = business.list_merges(**data)

#     return jsonify(message), status_code


# @app.route('/cubes/<cube_id>/items', methods=['GET'])
# def list_cube_items(cube_id):
#     data, status = validate(request.args.to_dict(), 'list_cube_items_form')

#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status_code = business.list_cube_items(cube_id, **data)

#     return jsonify(message), status_code


@app.route('/buckets', methods=['GET'])
def list_buckets():
    message, status_code = business.list_buckets()

    return jsonify(message), status_code

@app.route("/create-bucket", methods=["POST"])
def craete_bucket():
    # validate params
    data, status = validate(request.json, 'bucket')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_bucket(**data)
    return jsonify(message), status


# @app.route('/timeline', methods=['GET'])
# def list_timeline():
#     data, status = validate(request.args.to_dict(), 'list_timeline_form')

#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status_code = business.list_timeline(**data)

#     return jsonify(message), status_code


# @app.route('/cubes/<cube_id>/items/tiles', methods=['GET'])
# def list_items_tiles(cube_id):
#     message, status_code = business.list_cube_items_tiles(cube_id)

#     return jsonify(message), status_code


# @app.route('/cubes/<cube_id>/meta', methods=['GET'])
# def get_cube_meta(cube_id: str):
#     """Retrieve the meta information of a data cube such STAC provider used, collection, etc."""
#     message, status_code = business.get_cube_meta(cube_id)

#     return jsonify(message), status_code


# @app.route('/estimate-cost',methods=["POST"])
# def estimate_cost():
#     # validate params
#     data, status = validate(request.json, 'estimate_cost')
#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status = business.estimate_cost(**data)
#     return jsonify(message), status


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
