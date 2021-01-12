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


def process():
    item = dict(
        process_id=dict(type="string", empty=False, required=False),
        url_stac=dict(type="string", empty=False, required=True),
        bucket=dict(type="string", empty=False, required=True),
        tiles=dict(type="list", empty=False, required=True),
        collections=dict(type="list", empty=False, required=True),
        start_date=dict(type="date", coerce=to_date, empty=False, required=True),
        end_date=dict(type="date", coerce=to_date, empty=True, required=False),
        force=dict(type="boolean", required=False, default=False)
    )
    return item


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


def validate(data, type_schema):
    schema = eval('{}()'.format(type_schema))

    v = BDCValidator(schema)
    if not v.validate(data):
        return v.errors, False
    return data, True