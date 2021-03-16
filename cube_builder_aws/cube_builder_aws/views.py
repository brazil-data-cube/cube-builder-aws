#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import base64
import json

from flask import Blueprint, jsonify, request

from .controller import CubeController
from .forms import (BucketForm, CubeItemsForm, CubeStatusForm, DataCubeForm,
                    DataCubeMetadataForm, DataCubeProcessForm, GridRefSysForm,
                    PeriodForm, StartProcessForm)
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


# Cube Metadata
@bp.route("/cube-status", methods=["GET"])
def get_status():
    """Retrieve the cube processing state, which refers to total items and total to be done."""
    form = CubeStatusForm()

    args = request.args.to_dict()

    errors = form.validate(args)
    
    if errors:
        return errors, 400

    message, status = controller.get_cube_status(**args)
    return jsonify(message), status


@bp.route('/cubes', defaults=dict(cube_id=None), methods=['GET'])
@bp.route('/cubes/<cube_id>', methods=['GET'])
def list_cubes(cube_id):
    """List all data cubes available."""
    if cube_id is not None:
        message, status_code = controller.get_cube(cube_id)

    else:
        message, status_code = controller.list_cubes()

    return jsonify(message), status_code


@bp.route('/cubes', methods=['POST'])
def create_cube():
    """Define POST handler for datacube creation.
    Expects a JSON that matches with ``DataCubeForm``.
    """
    form = DataCubeForm()

    args = request.get_json()

    errors = form.validate(args)

    if errors:
        return errors, 400

    data = form.load(args)

    cubes, status = controller.create(data)

    return jsonify(cubes), status


@bp.route('/cubes/<cube_id>', methods=['PUT'])
def update_cube_matadata(cube_id):
    """Define PUT handler for datacube Updation.
    Expects a JSON that matches with ``DataCubeMetadataForm``.
    """
    form = DataCubeMetadataForm()

    args = request.get_json()

    errors = form.validate(args)

    if errors:
        return errors, 400

    data = form.load(args)

    message, status = CubeController.update(cube_id, data)

    return jsonify(message), status


@bp.route('/cubes/<cube_id>/meta', methods=['GET'])
def get_cube_meta(cube_id: str):
    """Retrieve the meta information of a data cube such STAC provider used, collection, etc."""
    message, status_code = controller.get_cube_meta(cube_id)

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/tiles', methods=['GET'])
def list_tiles(cube_id):
    """List all data cube tiles already done."""
    message, status_code = CubeController.list_tiles_cube(cube_id, only_ids=True)

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/tiles/geom', methods=['GET'])
def list_tiles_as_features(cube_id):
    """List all tiles as GeoJSON feature."""
    message, status_code = CubeController.list_tiles_cube(cube_id)

    return jsonify(message), status_code


@bp.route('/cubes/<cube_id>/items', methods=['GET'])
def list_cube_items(cube_id):
    """List all data cube items."""
    form = CubeItemsForm()

    args = request.args.to_dict()

    errors = form.validate(args)

    if errors:
        return errors, 400

    message, status_code = controller.list_cube_items(cube_id, **args)

    return jsonify(message), status_code


# Grid Ref Sys
@bp.route('/grids', defaults=dict(grs_id=None), methods=['GET'])
@bp.route('/grids/<grs_id>', methods=['GET'])
def list_grs_schemas(grs_id):
    """List all data cube Grids."""
    if grs_id is not None:
        result, status_code = CubeController.get_grs_schema(grs_id)
    else:
        result, status_code = CubeController.list_grs_schemas()

    return jsonify(result), status_code


@bp.route("/create-grs", methods=["POST"])
def craete_grs():
    """Create the grid reference system using HTTP Post method."""
    form = GridRefSysForm()

    args = request.get_json()

    errors = form.validate(args)

    if errors:
        return errors, 400

    cubes, status = CubeController.create_grs_schema(**args)

    return cubes, status


# Start Processing
@bp.route("/start", methods=["POST"])
def start():
    """Define POST handler for datacube execution.
    Expects a JSON that matches with ``DataCubeProcessForm``.
    """
    args = request.get_json()

    form = DataCubeProcessForm()

    errors = form.validate(args)

    if errors:
        return errors, 400

    data = form.load(args)

    controller = CubeController(url_stac=args['stac_url'], bucket=args['bucket'])
    message, status = controller.start_process(data)

    return jsonify(message), status


# Extras
@bp.route('/composite-functions', methods=['GET'])
def list_composite_functions():
    """List all data cube supported composite functions."""
    message, status_code = CubeController.list_composite_functions()

    return jsonify(message), status_code


@bp.route('/list-merges', methods=['GET'])
def list_merges():
    """Define POST handler for datacube execution.
    Expects a JSON that matches with ``DataCubeProcessForm``.
    """
    args = request.args

    res = controller.check_for_invalid_merges(**args)

    return res


@bp.route('/list-buckets', methods=['GET'])
def list_buckets():
    message, status_code = controller.list_buckets()

    return jsonify(message), status_code


@bp.route("/create-bucket", methods=["POST"])
def craete_bucket():
    form = CubeStatusForm()

    args = request.get_json()

    errors = form.validate(args)

    if errors:
        return errors, 400

    message, status = controller.create_bucket(**args)
    return jsonify(message), status


@bp.route('/list-periods', methods=['POST'])
def list_periods():
    """List data cube periods.
    The user must provide the following query-string parameters:
    - schema: Temporal Schema
    - step: Temporal Step
    - start_date: Start offset
    - last_date: End date offset
    """
    parser = PeriodForm()

    args = request.get_json()

    errors = parser.validate(args)

    if errors:
        return errors, 400

    return CubeController.generate_periods(**args)


# @bp.route('/estimate-cost',methods=["POST"])
# def estimate_cost():
#     # validate params
#     data, status = validate(request.json, 'estimate_cost')
#     if status is False:
#         return jsonify(json.dumps(data)), 400

#     message, status = controller.estimate_cost(**data)
#     return jsonify(message), status
