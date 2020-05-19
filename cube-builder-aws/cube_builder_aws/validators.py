"""
validation of controllers schemas
"""
from cerberus import Validator
from datetime import datetime

def to_date(s):
    return datetime.strptime(s, '%Y-%m-%d') if s else None

def to_bbox(s):
    bbox = s.split(',')
    if len(bbox) != 4: 
        return None
    return bbox

def create():
    item = {
        'datacube': {"type": "string", "empty": False, "required": True},
        'grs': {"type": "string", "empty": False, "required": True},
        'resolution': {"type": "integer", "empty": False, "required": True},
        'temporal_schema': {"type": "string", "empty": False, "required": True},
        'bands_quicklook': {"type": "list", "empty": False, "required": True},
        'bands': {"type": "list", "empty": False, "required": True},
        'license': {"type": "string", "empty": False, "required": True},
        'oauth_scope': {"type": "string", "empty": True, "required": False},
        'description': {"type": "string", "empty": False, "required": True}
    }
    return item

def process():
    item = {
        'url_stac': {"type": "string", "empty": False, "required": True},
        'bucket': {"type": "string", "empty": False, "required": True},
        'datacube': {"type": "string", "empty": False, "required": True},
        'tiles': {"type": "string", "empty": False, "required": True},
        'collections': {"type": "string", "empty": False, "required": True},
        'start_date': {"type": "date", "coerce": to_date, "empty": False, "required": True},
        'end_date': {"type": "date", "coerce": to_date, "empty": True, "required": False}
    }
    return item

def grs():
    item = {
        'name': {"type": "string", "empty": False, "required": True},
        'description': {"type": "string", "empty": False, "required": True},
        'projection': {"type": "string", "empty": False, "required": True, "allowed": ['aea', 'sinu', 'longlat']},
        'meridian': {"type": "float", "empty": False, "required": True},
        'degreesx': {"type": "float", "empty": False, "required": True},
        'degreesy': {"type": "float", "empty": False, "required": True},
        'bbox': {"type": "list", "empty": False, "required": True, "coerce": to_bbox}
    }
    return item

def raster_size():
    item = {
        'grs_schema': {"type": "string", "empty": False, "required": True},
        'resolution': {"type": "string", "empty": False, "required": True},
        'chunk_size_x': {"type": "float", "empty": False, "required": True},
        'chunk_size_y': {"type": "float", "empty": False, "required": True}
    }
    return item

def temporal_composition():
    item = {
        'temporal_schema': {"type": "string", "empty": False, "required": True, "allowed": ['A', 'M', 'S']},
        'temporal_composite_t': {"type": "string", "empty": True, "required": False},
        'temporal_composite_unit': {"type": "string", "empty": True, "required": False, "allowed": ['day', 'month']}
    }
    return item

def status():
    item = {
        'datacube': {"type": "string", "empty": False, "required": True}
    }
    return item


def list_merge_form():
    item = dict(
        data_cube=dict(type='string', empty=False, required=True),
        tile_id=dict(type='string', empty=False, required=True),
        start=dict(type='string', empty=False, required=True),
        end=dict(type='string', empty=False, required=True),
    )

    return item


def list_cube_items_form():
    return dict(
        bbox=dict(
            type='list',
            empty=False,
            required=False,
            coerce=to_bbox
        ),
        start=dict(type='string', empty=False, required=False),
        end=dict(type='string', empty=False, required=False),
        page=dict(type='integer', empty=False, required=True, default=1, coerce=int),
    )


def validate(data, type_schema):
    schema = eval('{}()'.format(type_schema))

    v = Validator(schema)
    if not v.validate(data):
        return v.errors, False
    return data, True