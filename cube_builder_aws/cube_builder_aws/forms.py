#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define Cube Builder AWS forms used to validate both data input and data serialization."""

from bdc_catalog.models import Collection, GridRefSys, db
from marshmallow import Schema, fields, pre_load
from marshmallow.validate import OneOf, Regexp, ValidationError
from marshmallow_sqlalchemy.schema import ModelSchema
from rasterio.dtypes import dtype_ranges


class CollectionForm(ModelSchema):
    """Form definition for Model Collection."""

    class Meta:
        """Internal meta information of Form interface."""

        model = Collection
        sqla_session = db.session
        exclude = ('extent', )


class GridRefSysForm(ModelSchema):
    """Form definition for the model GrsSchema."""

    id = fields.String(dump_only=True)
    name = fields.String(required=True, load_only=True)
    projection = fields.String(required=True, load_only=True)
    meridian = fields.Integer(required=True, load_only=True)
    degreesx = fields.Float(required=True, load_only=True)
    degreesy = fields.Float(required=True, load_only=True)
    bbox = fields.String(required=True, load_only=True)
    srid = fields.Integer(required=False, load_only=True)

    class Meta:
        """Internal meta information of form interface."""

        model = GridRefSys
        sqla_session = db.session
        exclude = ('table_id', )


INVALID_CUBE_NAME = 'Invalid data cube name. Expected only letters and numbers.'
SUPPORTED_DATA_TYPES = list(dtype_ranges.keys())


class BandDefinition(Schema):
    """Define a simple marshmallow structure for data cube bands on creation."""

    name = fields.String(required=True, allow_none=False)
    common_name = fields.String(required=True, allow_none=False)
    data_type = fields.String(required=True, allow_none=False, validate=OneOf(SUPPORTED_DATA_TYPES))
    nodata = fields.Integer(required=False, allow_none=False)
    metadata = fields.Dict(required=False, allow_none=False)


class QAConfidence(Schema):
    """Define that will discard all cloud values which has confidence greater or equal MEDIUM.
    qa = QAConfidence(cloud='cloud >= MEDIUM', cloud_shadow=None, cirrus=None, snow=None, landsat_8=True)."""
    
    cloud = fields.String(required=False, allow_none=True)
    cloud_shadow = fields.String(required=False, allow_none=True)
    cirrus = fields.String(required=False, allow_none=True)
    snow = fields.String(required=False, allow_none=True)


class CustomMaskDefinition(Schema):
    """Define a custom mask."""

    clear_data = fields.List(fields.Integer, required=True, allow_none=False)
    not_clear_data = fields.List(fields.Integer, required=True, allow_none=False)
    saturated_data = fields.List(fields.Integer, required=True, allow_none=False)
    nodata = fields.Integer(required=True, allow_none=False)
    bits = fields.Boolean(required=False, allow_none=False, default=False)
    confidence = fields.Nested(QAConfidence)


class StacDefinitionForm(Schema):
    """Define parser for stac and collection used to create cube."""

    url = fields.String(required=True, allow_none=False)
    token = fields.String(required=False, allow_none=True)
    collection = fields.String(required=True, allow_none=False)


class LandsatHarmonization(Schema):
    """Define parser for params of the landsat harmonization."""

    apply = fields.Boolean(required=False, allow_none=False, default=False)
    bucket_angle_bands = fields.String(required=False, allow_none=True)
    build_provenance = fields.Boolean(required=False, allow_none=False, default=False)
    datasets = fields.List(fields.String(required=True, allow_none=False))
    map_bands = fields.Dict(required=False, allow_none=False)


class DataCubeForm(Schema):
    """Define parser for datacube creation."""

    datacube = fields.String(required=True, allow_none=False, validate=Regexp('^[a-zA-Z0-9-]*$', error=INVALID_CUBE_NAME))
    grs = fields.String(required=True, allow_none=False)
    resolution = fields.Integer(required=True, allow_none=False)
    temporal_composition = fields.Dict(required=True, allow_none=False)
    bands_quicklook = fields.List(fields.String, required=True, allow_none=False)
    composite_function = fields.String(required=True, allow_none=False)
    bands = fields.Nested(BandDefinition, required=True, allow_none=False, many=True)
    quality_band = fields.String(required=True, allow_none=False)
    indexes = fields.Nested(BandDefinition, many=True)
    metadata = fields.Dict(required=True, allow_none=True)
    description = fields.String(required=True, allow_none=False)
    version = fields.Integer(required=True, allow_none=False, default=1)
    title = fields.String(required=True, allow_none=False)
    # Set cubes as public by default.
    public = fields.Boolean(required=False, allow_none=False, default=True)
    # Is Data cube generated from Combined Collections?
    is_combined = fields.Boolean(required=False, allow_none=False, default=False)
    parameters = fields.Dict(
        mask = fields.Nested(CustomMaskDefinition),
        stac_list = fields.List(fields.Nested(StacDefinitionForm)),
        landsat_harmonization = fields.Nested(LandsatHarmonization)
    )

    @pre_load
    def validate_indexes(self, data, **kwargs):
        """Ensure that both indexes and quality band is present in attribute 'bands'.
        Seeks for quality_band in attribute 'bands' and set as `common_name`.
        Raises:
            ValidationError when a band inside indexes or quality_band is duplicated with attribute bands.
        """
        indexes = data['indexes']

        band_names = [b['name'] for b in data['bands']]

        for band_index in indexes:
            if band_index['name'] in band_names:
                raise ValidationError(f'Duplicated band name in indices {band_index["name"]}')

        if 'quality_band' in data:
            if data['quality_band'] not in band_names:
                raise ValidationError(f'Quality band "{data["quality_band"]}" not found in key "bands"')

            band = next(filter(lambda band: band['name'] == data['quality_band'], data['bands']))
            band['common_name'] = 'quality'

        if 'temporal_schema' in data:
            import json
            import pkgutil

            import bdc_catalog
            from jsonschema import draft7_format_checker, validate
            content = pkgutil.get_data(bdc_catalog.__name__, 'jsonschemas/collection-temporal-composition-schema.json')
            schema = json.loads(content)
            try:
                schema['$id'] = schema['$id'].replace('#', '')
                validate(instance=data['temporal_schema'], schema=schema, format_checker=draft7_format_checker)
            except Exception as e:
                print(e)
                raise

        return data


class DataCubeMetadataForm(Schema):
    """Define parser for datacube updation."""

    metadata = fields.Dict(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=False)
    title = fields.String(required=False, allow_none=False)
    public = fields.Boolean(required=False, allow_none=False, default=True)
    

class DataCubeProcessForm(Schema):
    """Define parser for datacube generation."""

    datacube = fields.String(required=True, allow_none=False)
    datacube_version = fields.Integer(required=True, allow_none=False)
    stac_list = fields.List(fields.Nested(StacDefinitionForm))
    bucket = fields.String(required=True, allow_none=False)
    tiles = fields.List(fields.String, required=True, allow_none=False)
    start_date = fields.Date()
    end_date = fields.Date()
    force = fields.Boolean(required=False, default=False)
    shape = fields.List(fields.Integer(required=False))
    # Reuse data cube from another data cube
    reuse_from = fields.String(required=False, allow_none=True)
    indexes_only_regular_cube = fields.Boolean(required=False, allow_none=True, default=False)


class PeriodForm(Schema):
    """Define parser for Data Cube Periods."""

    schema = fields.String(required=True, allow_none=False)
    step = fields.Integer(required=True)
    unit = fields.String(required=True)
    start_date = fields.String(required=False)
    last_date = fields.String(required=False)
    cycle = fields.Dict(required=False, allow_none=True)
    intervals = fields.List(fields.String, required=False, allow_none=True)


class CubeStatusForm(Schema):
    """Parser for access data cube status resource."""

    cube_name = fields.String(required=True, allow_none=False)


class CubeItemsForm(Schema):
    """Parser for access data cube items resource."""

    tiles = fields.String(required=False)
    bbox = fields.String(required=False)
    start = fields.String(required=False)
    end = fields.String(required=False)
    page = fields.Integer(required=False)
    per_page = fields.Integer(required=False)


class BucketForm(Schema):
    """Parser for create bucket."""

    name = fields.String(required=False)
    requester_pay = fields.Boolean(required=False, default=True)
