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
from rasterio.dtypes import dtype_ranges

def to_date(s):
    return datetime.strptime(s, '%Y-%m-%d') if s else None

def to_bbox(s):
    bbox = s.split(',')
    if len(bbox) != 4: 
        return None
    return bbox


def index_band():
    return dict(
        type="dict",
        schema=dict(
            name=dict(type='string', empty=False, required=True),
            common_name=dict(type='string', empty=False, required=True),
            data_type=dict(type='string', empty=False, required=True, allowed=list(dtype_ranges.keys()))
        )
    )


def create():
    item = dict(
        datacube=dict(type='string', empty=False, required=True),
        grs=dict(type='string', empty=False, required=True),
        resolution=dict(type='float', empty=False, required=True),
        temporal_schema=dict(type='string', empty=False, required=True),
        bands_quicklook=dict(type='list', empty=False, required=True),
        composite_function=dict(type='list', empty=False, required=True, allowed=['IDENTITY', 'STK', 'MED']),
        bands=dict(type='list', empty=False, required=True, schema=index_band()),
        indexes=dict(type='list', empty=True, required=False, schema=index_band(), check_with='band_uniqueness'),
        quality_band=dict(type='string', empty=True, required=False, check_with='band_uniqueness'),
        license=dict(type='string', empty=True, required=False),
        oauth_scope=dict(type='string', empty=True, required=False),
        description=dict(type='string', empty=True, required=False)
    )
    return item

def process():
    item = {
        'process_id': {"type": "string", "empty": False, "required": False},
        'datacube': {"type": "string", "empty": False, "required": False},
        'url_stac': {"type": "string", "empty": False, "required": True},
        'bucket': {"type": "string", "empty": False, "required": True},
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
        quantity_indexes=dict(type='integer', empty=False, required=True),
        t_schema=dict(type='string', empty=False, required=False),
        t_step=dict(type='integer', empty=False, required=True, default=1, coerce=int)
    )


class BDCValidator(Validator):
    def _check_with_band_uniqueness(self, field, value):
        bands = self.document['bands']
        band_names = [b['name'] for b in bands]

        if field == 'indexes':
            for index in value:
                if index['name'] in band_names:
                    self._error(field, f'Duplicated band name in indices {index["name"]}')
        elif field == 'quality_band':
            if value not in band_names:
                self._error(field, f'Quality band "{value}" not found in key "bands"')


def validate(data, type_schema):
    schema = eval('{}()'.format(type_schema))

    v = BDCValidator(schema)
    if not v.validate(data):
        return v.errors, False
    return data, True