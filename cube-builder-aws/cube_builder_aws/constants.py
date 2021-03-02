#
#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define the Brazil Data Cube Builder constants."""

SRID_BDC_GRID = 100001

APPLICATION_ID = 2

CLEAR_OBSERVATION_NAME = 'CLEAROB'

CLEAR_OBSERVATION_ATTRIBUTES = dict(
    name=CLEAR_OBSERVATION_NAME,
    description='Clear Observation Count.',
    data_type='uint8',
    min_value=0,
    max_value=255,
    nodata=0,
    scale=1,
    common_name='ClearOb',
)

TOTAL_OBSERVATION_NAME = 'TOTALOB'

TOTAL_OBSERVATION_ATTRIBUTES = dict(
    name=TOTAL_OBSERVATION_NAME,
    description='Total Observation Count',
    data_type='uint8',
    min_value=0,
    max_value=255,
    nodata=0,
    scale=1,
    common_name='TotalOb',
)

PROVENANCE_NAME = 'PROVENANCE'

PROVENANCE_ATTRIBUTES = dict(
    name=PROVENANCE_NAME,
    description='Provenance value Day of Year',
    data_type='int16',
    min_value=1,
    max_value=366,
    nodata=-1,
    scale=1,
    common_name='Provenance',
)

# Band for Combined Collections
DATASOURCE_NAME = 'DATASOURCE'

DATASOURCE_ATTRIBUTES = dict(
    name=DATASOURCE_NAME,
    description='Data set value',
    data_type='uint8',
    min_value=0,
    max_value=254,
    nodata=255,
    scale=1,
    common_name='datasource',
)

CENTER_WAVELENGTH = dict(
    cbers=dict(
        band13=0.485,
        band14=0.555,
        band15=0.660,
        band16=0.830
    ),
    landsat=dict(
        band1=0.443,
        band2=0.4825,
        band3=0.5625,
        band4=0.655,
        band5=0.865,
        band6=1.61,
        band7=2.2
    ),
    sentinel=dict(
        band1=0.4427,
        band2=0.4924,
        band3=0.5598,
        band4=0.6646,
        band5=0.7041,
        band6=0.7405,
        band7=0.7828,
        band8=0.8328,
        band8a=0.8647,
        band9=0.9451,
        band10=0.3735,
        band11=0.6137,
        band12=0.2024
    )
)

FULL_WIDTH_HALF_MAX = dict(
    cbers=dict(
        band13=0.035,
        band14=0.035,
        band15=0.030,
        band16=0.060
    ),
    landsat=dict(
        band1=0.01,
        band2=0.0325,
        band3=0.0375,
        band4=0.025,
        band5=0.020,
        band6=0.05,
        band7=0.1
    ),
    sentinel=dict(
        band1=0.021,
        band2=0.066,
        band3=0.036,
        band4=0.031,
        band5=0.015,
        band6=0.015,
        band7=0.020,
        band8=0.106,
        band8a=0.021,
        band9=0.020,
        band10=0.031,
        band11=0.091,
        band12=0.175
    )
)

RESOLUTION_BY_SATELLITE = {
    'CBERS-4-MUX': 20,
    'CBERS-4-WFI': 64,
    'MODIS': 231,
    'LANDSAT': 30,
    'SENTINEL-2': 10
}

REVISIT_BY_SATELLITE = {
    'CBERS-4-MUX': 26,
    'CBERS-4-WFI': 6,
    'MODIS': 2,
    'LANDSAT': 16,
    'SENTINEL-2': 5
}

COG_MIME_TYPE = 'image/tiff; application=geotiff; profile=cloud-optimized'

PNG_MIME_TYPE = 'image/png'

SRID_ALBERS_EQUAL_AREA = 100001