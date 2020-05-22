# app.py

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
from bdc_db import BDCDatabase
from config import USER, PASSWORD, HOST, DBNAME

from cube_builder_aws.business import CubeBusiness
from cube_builder_aws.validators import validate
from cube_builder_aws.version import __version__


class ImprovedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        if isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        return super(ImprovedJSONEncoder, self).default(o)


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{}:{}@{}:5432/{}'.format(
   USER, PASSWORD, HOST, DBNAME
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['REDOC'] = {
    'title': 'Cube Builder AWS',
    'spec_route': '/docs'
}
app.json_encoder = ImprovedJSONEncoder
CORS(app)

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


@app.route("/start", methods=["POST"])
def start():
    # validate params
    data, status = validate(request.json, 'process')
    if status is False:
        return jsonify(json.dumps(data)), 400

    business = CubeBusiness(url_stac=data['url_stac'], bucket=data['bucket'])
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


@app.route("/raster-size", methods=["GET"])
def list_raster_size():
    message, status_code = business.list_raster_size()

    return jsonify(message), status_code


@app.route("/cube-status", methods=["GET"])
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


@app.route('/cubes/<cube_id>/tiles', methods=['GET'])
def list_tiles_as_features(cube_id):
    message, status_code = business.list_tiles_cube(cube_id)

    return jsonify(message), status_code


@app.route('/grs', defaults=dict(grs_id=None), methods=['GET'])
@app.route('/grs/<grs_id>', methods=['GET'])
def list_grs_schemas(grs_id):
    if grs_id is not None:
        message, status_code = business.get_grs_schema(grs_id)
    else:
        message, status_code = business.list_grs_schemas()

    return jsonify(message), status_code


@app.route('/temporal-composition', methods=['GET'])
def list_temporal_composition():
    message, status_code = business.list_temporal_composition()

    return jsonify(message), status_code


@app.route("/create-temporal-composition", methods=["POST"])
def craete_temporal_composition():
    # validate params
    data, status = validate(request.json, 'temporal_composition')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_temporal_composition(**data)
    return jsonify(message), status


@app.route('/composite-functions', methods=['GET'])
def list_composite_functions():
    message, status_code = business.list_composite_functions()

    return jsonify(message), status_code


@app.route('/list-buckets', methods=['GET'])
def list_buckets():
    message, status_code = business.list_buckets()

    return jsonify(message), status_code


@app.route('/list-merges', methods=['GET'])
def list_merges():
    data, status = validate(request.args.to_dict(), 'list_merge_form')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status_code = business.list_merges(**data)

    return jsonify(message), status_code


@app.route('/cubes/<cube_id>/items', methods=['GET'])
def list_cube_items(cube_id):
    data, status = validate(request.args.to_dict(), 'list_cube_items_form')

    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status_code = business.list_cube_items(cube_id, **data)

    return jsonify(message), status_code


@app.route("/create-bucket", methods=["POST"])
def craete_bucket():
    # validate params
    data, status = validate(request.json, 'bucket')
    if status is False:
        return jsonify(json.dumps(data)), 400

    message, status = business.create_bucket(**data)
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
