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
        'composite_function_list': {"type": "list", "empty": False, "required": True},
        'bands': {"type": "list", "empty": False, "required": True},
        'license': {"type": "string", "empty": False, "required": True},
        'oauth_scope': {"type": "string", "empty": True, "required": False},
        'description': {"type": "string", "empty": False, "required": True}
    }
    return item

def process():
    item = {
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
        'raster_size_x': {"type": "float", "empty": False, "required": True},
        'raster_size_y': {"type": "float", "empty": False, "required": True},
        'chunk_size_x': {"type": "float", "empty": False, "required": True},
        'chunk_size_y': {"type": "float", "empty": False, "required": True}
    }
    return item


def validate(data, type_schema):
    schema = eval('{}()'.format(type_schema))

    v = Validator(schema)
    if not v.validate(data):
        return v.errors, False
    return data, True