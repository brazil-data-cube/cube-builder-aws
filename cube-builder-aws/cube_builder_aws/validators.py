#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

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


def indice_band():
    return dict(
        name=dict(type='string', empty=False, required=True),
        common_name=dict(type='string', empty=False, required=True),
        dtype=dict(type='string', empty=False, required=True, allowed=['int16', 'Uint16'])
    )


def create():
    item = dict(
        datacube=dict(type='string', empty=False, required=True),
        grs=dict(type='string', empty=False, required=True),
        resolution=dict(type='string', empty=False, required=True),
        temporal_schema=dict(type='string', empty=False, required=True),
        bands_quicklook=dict(type='list', empty=False, required=True),
        composite_function=dict(type='string', empty=False, required=True, allowed=['IDENTITY', 'STK', 'MED']),
        bands=dict(type='list', empty=False, required=True, schema=indice_band()),
        indices=dict(type='list', empty=True, required=False, schema=indice_band()),
        quality_band=dict(type='string', empty=True, required=False),
        license=dict(type='string', empty=True, required=False),
        oauth_scope=dict(type='string', empty=True, required=False),
        description=dict(type='string', empty=True, required=False)
    )
    return item

def process():
    item = {
        'url_stac': {"type": "string", "empty": False, "required": True},
        'bucket': {"type": "string", "empty": False, "required": True},
        'datacube': {"type": "string", "empty": False, "required": True},
        'tiles': {"type": "list", "empty": False, "required": True},
        'collections': {"type": "string", "empty": False, "required": True},
        'satellite': {"type": "string", "empty": False, "required": True},
        'start_date': {"type": "date", "coerce": to_date, "empty": False, "required": True},
        'end_date': {"type": "date", "coerce": to_date, "empty": True, "required": False},
        'force': {'type': 'boolean', 'required': False, 'default': False}
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
        'resolution': {"type": "integer", "empty": False, "required": True},
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
        tiles=dict(type='string', empty=False, required=False),
        start=dict(type='string', empty=False, required=False),
        end=dict(type='string', empty=False, required=False),
        page=dict(type='integer', empty=False, required=True, default=1, coerce=int),
    )


def bucket():
    return dict(
        name=dict(type='string', empty=False, required=False),
        requester_pay=dict(type='boolean', empty=False, required=False, default=True)
    )


def list_timeline_form():
    return dict(
        start=dict(type='string', empty=False, required=False),
        end=dict(type='string', empty=False, required=False),
        schema=dict(type='string', empty=False, required=False),
        step=dict(type='integer', empty=False, required=True, default=1, coerce=int)
    )


def estimate_cost():
    return dict(
        start_date=dict(type='date', empty=False, required=True, coerce=to_date),
        last_date=dict(type='date', empty=False, required=True, coerce=to_date),
        satellite=dict(type='string', empty=False, required=True),
        resolution=dict(type='integer', empty=False, required=True),
        grid=dict(type='string', empty=False, required=True),
        quantity_bands=dict(type='integer', empty=False, required=True),
        quantity_tiles=dict(type='integer', empty=False, required=True),
        t_schema=dict(type='string', empty=False, required=False),
        t_step=dict(type='integer', empty=False, required=True, default=1, coerce=int)
    )


def validate(data, type_schema):
    schema = eval('{}()'.format(type_schema))

    v = Validator(schema)
    if not v.validate(data):
        return v.errors, False
    return data, True