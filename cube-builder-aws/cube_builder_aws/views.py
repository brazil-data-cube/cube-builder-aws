#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

# 3rdparty
from flask import Blueprint, jsonify, request
import json

from .controller import CubeController
from .forms import (CubeStatusForm)
# TODO: remove cerberus and validators file
from .validators import validate
from .version import __version__

controller = CubeController()

bp = Blueprint('cubes', import_name=__name__)


#########################################
# REQUEST HTTP -> from API Gateway
#########################################
@bp.route("/", methods=["GET"])
def status():
    return jsonify(
        message = 'Running',
        description = 'Cube Builder AWS',
        version = __version__
    ), 200


@bp.route("/cube-status", methods=["GET"])
def get_status():
    """Retrieve the cube processing state, which refers to total items and total to be done."""
    form = CubeStatusForm()

    args = request.args.to_dict()

    errors = form.validate(args)

    message, status = controller.get_cube_status(**args)
    return jsonify(message), status


@bp.route("/create-grs", methods=["POST"])
def craete_grs():
    # validate params
    data, status = validate(request.json, 'grs')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = controller.create_grs(**data)
    return jsonify(message), status


@bp.route('/composite-functions', methods=['GET'])
def list_composite_functions():
    message, status_code = controller.list_composite_functions()

    return jsonify(message), status_code


@bp.route("/create-cube", methods=["POST"])
def create():
    # validate params
    data, status = validate(request.json, 'create')
    if status is False:
        return jsonify(data), 400

    cubes, status = controller.create_cube(data)
    return jsonify(
        message = 'Cube created',
        cubes = cubes
    ), status


@bp.route("/start-cube", methods=["POST"])
def start():
    # validate params
    data, status = validate(request.json, 'process')
    if status is False:
        return jsonify(json.dumps(data)), 400

    controller = CubeController(url_stac=data['url_stac'], bucket=data['bucket'])
    message, status = controller.start_process(data)
    return jsonify(message), status



@bp.route('/cubes', defaults=dict(cube_id=None), methods=['GET'])
@bp.route('/cubes/<cube_id>', methods=['GET'])
def list_cubes(cube_id):
    if cube_id is not None:
        message, status_code = controller.get_cube(cube_id)
    else:
        message, status_code = controller.list_cubes()

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/tiles', methods=['GET'])
def list_tiles_as_features(cube_id):
    message, status_code = controller.list_tiles_cube(cube_id)

    return jsonify(message), status_code


@bp.route('/grids', defaults=dict(grs_id=None), methods=['GET'])
@bp.route('/grids/<grs_id>', methods=['GET'])
def list_grs_schemas(grs_id):
    if grs_id is not None:
        message, status_code = controller.get_grs_schema(grs_id)
    else:
        message, status_code = controller.list_grs_schemas()

    return jsonify(message), status_code


@bp.route('/list-merges', methods=['GET'])
def list_merges():
    data, status = validate(request.args.to_dict(), 'list_merge_form')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status_code = controller.list_merges(**data)

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/items', methods=['GET'])
def list_cube_items(cube_id):
    data, status = validate(request.args.to_dict(), 'list_cube_items_form')

    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status_code = controller.list_cube_items(cube_id, **data)

    return jsonify(message), status_code


@bp.route('/buckets', methods=['GET'])
def list_buckets():
    message, status_code = controller.list_buckets()

    return jsonify(message), status_code

@bp.route("/create-bucket", methods=["POST"])
def craete_bucket():
    # validate params
    data, status = validate(request.json, 'bucket')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = controller.create_bucket(**data)
    return jsonify(message), status


@bp.route('/timeline', methods=['GET'])
def list_timeline():
    data, status = validate(request.args.to_dict(), 'list_timeline_form')

    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status_code = controller.list_timeline(**data)

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/items/tiles', methods=['GET'])
def list_items_tiles(cube_id):
    message, status_code = controller.list_cube_items_tiles(cube_id)

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/meta', methods=['GET'])
def get_cube_meta(cube_id: str):
    """Retrieve the meta information of a data cube such STAC provider used, collection, etc."""
    message, status_code = controller.get_cube_meta(cube_id)

    return jsonify(message), status_code


# @bp.route('/estimate-cost',methods=["POST"])
# def estimate_cost():
#     # validate params
#     data, status = validate(request.json, 'estimate_cost')
#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status = controller.estimate_cost(**data)
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

        message = controller.continue_process_stream(params_list)
        return message
