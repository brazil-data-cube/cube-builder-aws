#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import datetime
import hashlib
import os
from typing import List, Tuple

import numpy
import rasterio
import rasterio.features
import rasterio.warp
import shapely.geometry
from bdc_catalog.models import db
from bdc_catalog.utils import \
    multihash_checksum_sha256 as _multihash_checksum_sha256
from dateutil.relativedelta import relativedelta
from flask import abort
from geoalchemy2.shape import from_shape
from numpngw import write_png
from rasterio.io import MemoryFile
from rasterio.warp import Resampling
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

#############################
from .interpreter import execute


def get_date(str_date):
    return datetime.datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')


#############################
def encode_key(activity, keylist):
    dynamoKey = ''
    for key in keylist:
        dynamoKey += activity[key]
    return dynamoKey


############################
def qa_statistics(raster, mask, blocks) -> Tuple[float, float]:
    """Retrieve raster statistics efficacy and not clear ratio, based in Fmask values.

    Notes:
        Values 0 and 1 are considered `clear data`.
        Values 2 and 4 are considered as `not clear data`
        The values for snow `3` and nodata `255` is not used to count efficacy and not clear ratio
    """
    # Compute how much data is for each class. It will be used as image area
    clear_pixels = 0
    not_clear_pixels = 0

    for _, window in blocks:
        row_offset = window.row_off + window.height
        col_offset = window.col_off + window.width

        raster_block = raster[window.row_off: row_offset, window.col_off: col_offset]

        clear_pixels += raster_block[numpy.where(numpy.isin(raster_block, mask['clear_data']))].size
        not_clear_pixels += raster_block[numpy.where(numpy.isin(raster_block, mask['not_clear_data']))].size

    # Image area is everything, except nodata.
    image_area = clear_pixels + not_clear_pixels

    not_clear_ratio = 100
    if image_area != 0:
        not_clear_ratio = round(100. * not_clear_pixels / image_area, 2)

    efficacy = round(100. * clear_pixels / raster.size, 2)

    return efficacy, not_clear_ratio


############################
def getMask(raster, mask, blocks):
    """Retrieves and re-sample quality raster to well-known values used in Brazil Data Cube.

    Args:
        raster - Raster with Quality band values

    Returns:
        Tuple containing formatted quality raster, efficacy and cloud ratio, respectively
    """
    rastercm = raster

    efficacy, cloudratio = qa_statistics(rastercm, mask=mask, blocks=blocks)

    return rastercm.astype(numpy.uint8), efficacy, cloudratio


############################
def generateQLook(generalSceneId, qlfiles):
    profile = None
    with rasterio.open(qlfiles[0]) as src:
        profile = src.profile

    numlin = 768
    numcol = int(float(profile['width'])/float(profile['height'])*numlin)
    image = numpy.ones((numlin,numcol,len(qlfiles),), dtype=numpy.uint8)
    pngname = '/tmp/{}.png'.format(generalSceneId)

    nb = 0
    for file in qlfiles:
        with rasterio.open(file) as src:
            raster = src.read(1, out_shape=(numlin, numcol))
            # Rescale to 0-255 values
            nodata = raster <= 0
            if raster.min() != 0 or raster.max() != 0:
                raster = raster.astype(numpy.float32)/10000.*255.
                raster[raster>255] = 255
            image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
            nb += 1

    write_png(pngname, image, transparent=(0, 0, 0))
    return pngname


#############################
class DataCubeFragments(list):
    """Parse a data cube name and retrieve their parts.
    A data cube is composed by the following structure:
    ``Collections_Resolution_TemporalPeriod_CompositeFunction``.
    An IDT data cube does not have TemporalPeriod and CompositeFunction.
    Examples:
        >>> # Parse Sentinel 2 Monthly MEDIAN
        >>> cube_parts = DataCubeFragments('S2_10_1M_MED') # ['S2', '10', '1M', 'MED']
        >>> cube_parts.composite_function
        ... 'MED'
        >>> # Parse Sentinel 2 IDENTITY
        >>> cube_parts = DataCubeFragments('S2_10') # ['S2', '10']
        >>> cube_parts.composite_function
        ... 'IDT'
        >>> DataCubeFragments('S2-10') # ValueError Invalid data cube name
    """

    def __init__(self, datacube: str):
        """Construct a Data Cube Fragments parser.
        Exceptions:
            ValueError when data cube name is invalid.
        """
        cube_fragments = self.parse(datacube)

        self.datacube = '_'.join(cube_fragments)

        super(DataCubeFragments, self).__init__(cube_fragments)

    @staticmethod
    def parse(datacube: str) -> List[str]:
        """Parse a data cube name."""
        cube_fragments = datacube.split('_')

        if len(cube_fragments) > 4 or len(cube_fragments) < 2:
            abort(400, 'Invalid data cube name. "{}"'.format(datacube))

        return cube_fragments

    def __str__(self):
        """Retrieve the data cube name."""
        return self.datacube

    @property
    def composite_function(self):
        """Retrieve data cube composite function based.
        TODO: Add reference to document User Guide - Convention Data Cube Names
        """
        if len(self) < 4:
            return 'IDT'

        return self[-1]
        

def get_or_create_model(model_class, defaults=None, **restrictions):
    """Define a utility method for looking up an object with the given restrictions, creating one if necessary.
    Args:
        model_class (BaseModel) - Base Model of Brazil Data Cube DB
        defaults (dict) - Values to fill out model instance
        restrictions (dict) - Query Restrictions
    Returns:
        BaseModel Retrieves model instance
    """
    instance = db.session.query(model_class).filter_by(**restrictions).first()

    if instance:
        return instance, False

    params = dict((k, v) for k, v in restrictions.items())

    params.update(defaults or {})
    instance = model_class(**params)

    db.session.add(instance)

    return instance, True

def get_cube_name(cube, function=None):
    if not function or function.upper() == 'IDT':
        return '_'.join(cube.split('_')[:-1])
    else:
        return '{}_{}'.format(cube, function)


def get_cube_parts(datacube: str) -> List[str]:
    """Build a `DataCubeFragments` and validate data cube name policy."""
    return DataCubeFragments(datacube)


############################
def generate_hash_md5(word):
    result = hashlib.md5(word.encode())
    return result.hexdigest()


############################
def create_cog_in_s3(services, profile, path, raster, is_quality, nodata, bucket_name):
    with MemoryFile() as dst_file:
        with MemoryFile() as memfile:
            with memfile.open(**profile) as mem:
                if is_quality:
                    mem.nodata = nodata
                mem.write_band(1, raster)
                
                dst_profile = cog_profiles.get("deflate")        
                cog_translate(
                    mem,
                    dst_file.name,
                    dst_profile,
                    in_memory=True,
                    quiet=True
                )

        services.upload_fileobj_S3(dst_file, path, {'ACL': 'public-read'}, bucket_name=bucket_name)
    return True


class AutoCloseDataSet:
    def __init__(self, file_path: str, mode: str = 'r', **options):
        self.options = options
        self.mode = mode
        self.dataset = rasterio.open(str(file_path), mode=mode, **options)

    def __del__(self):
        """Close dataset on delete object."""
        self.close()

    def close(self):
        """Close rasterio data set."""
        if not self.dataset.closed:
            self.dataset.close()


############################
def create_index(services, index, band_expressions, bands, bucket_name):
    """Generate data cube custom bands based in string-expression.

    This method seeks for custom bands on Collection Band definition. A custom band must have
    `metadata` property filled out according the ``bdc_catalog.jsonschemas.band-metadata.json``.

    Args:
        services - AWS service wrapper
        index - The band name
        band_expressions - Map of band expressions
        bands - Map of data cube bands path
        bucket_name - Bucket name where the data cube is stored.

    Raises:
        RuntimeError when an error occurs while interpreting the band expression in Python Virtual Machine.

    Returns:
        A dict values with generated bands.
    """
    prefix = services.get_s3_prefix(bucket_name)

    map_data_set_context = dict()
    band_definition = band_expressions[index]
    band_expression = band_definition['expression']['value']
    band_data_type = band_definition['data_type']
    data_type_info = numpy.iinfo(band_data_type)
    data_type_max_value = data_type_info.max
    data_type_min_value = data_type_info.min

    ref_file = None
    profile = blocks = None

    for _band, relative_path in bands.items():
        absolute_path = os.path.join(prefix, str(relative_path))
        map_data_set_context[_band] = AutoCloseDataSet(absolute_path)

        if profile is None:
            profile = map_data_set_context[_band].dataset.profile
            blocks = map_data_set_context[_band].dataset.block_windows()
            ref_file = relative_path

    file_path = '_'.join(ref_file.split('_')[:-1]) + '_{}.tif'.format(index)
    profile['dtype'] = band_data_type
    raster = numpy.full((profile['height'], profile['width']), dtype=band_data_type, fill_value=profile['nodata'])

    for _, window in blocks:
        row_offset = window.row_off + window.height
        col_offset = window.col_off + window.width

        machine_context = {
            k: ds.dataset.read(1, masked=True, window=window).astype(numpy.float32)
            for k, ds in map_data_set_context.items()
        }

        expr = f'{index} = {band_expression}'
        result = execute(expr, context=machine_context)
        raster_block = result[index]
        raster_block[raster_block == numpy.ma.masked] = profile['nodata']
        # Persist the expected band data type to cast value safely.
        # TODO: Should we use consider band min_value/max_value?
        raster_block[raster_block < data_type_min_value] = data_type_min_value
        raster_block[raster_block > data_type_max_value] = data_type_max_value

        raster[window.row_off: row_offset, window.col_off: col_offset] = raster_block

    create_cog_in_s3(services, profile, file_path, raster.astype(numpy.int16), False, None, bucket_name)


############################
def format_version(version, prefix='v'):
    return f'{prefix}{version:03d}'


def multihash_checksum_sha256(services, bucket_name, entry) -> str:
    obj = services.get_object(key=entry, bucket_name=bucket_name)

    return _multihash_checksum_sha256(obj['Body'])


############################
def create_asset_definition(services, bucket_name: str, href: str, mime_type: str, role: List[str], absolute_path: str,
                            created=None, is_raster=False):
    """Create a valid asset definition for collections.
    TODO: Generate the asset for `Item` field with all bands
    Args:
        href - Relative path to the asset
        mime_type - Asset Mime type str
        role - Asset role. Available values are: ['data'], ['thumbnail']
        absolute_path - Absolute path to the asset. Required to generate check_sum
        created - Date time str of asset. When not set, use current timestamp.
        is_raster - Flag to identify raster. When set, `raster_size` and `chunk_size` will be set to the asset.
    """
    fmt = '%Y-%m-%dT%H:%M:%S'
    _now_str = datetime.datetime.utcnow().strftime(fmt)

    if created is None:
        created = _now_str
    elif isinstance(created, datetime.datetime):
        created = created.strftime(fmt)

    file_obj = services.s3_file_exists(bucket_name=bucket_name, key=href)
    size = file_obj['ContentLength']

    asset = {
        'href': absolute_path,
        'type': mime_type,
        'bdc:size': size,
        'checksum:multihash': multihash_checksum_sha256(services=services, bucket_name=bucket_name, entry=href),
        'roles': role,
        'created': created,
        'updated': _now_str
    }

    geom = None
    min_convex_hull = None

    if is_raster:
        with rasterio.open(f's3://{absolute_path}') as data_set:
            asset['bdc:raster_size'] = dict(
                x=data_set.shape[1],
                y=data_set.shape[0],
            )

            _geom = shapely.geometry.mapping(shapely.geometry.box(*data_set.bounds))
            geom_shape = shapely.geometry.shape(rasterio.warp.transform_geom(data_set.crs, 'EPSG:4326', _geom))
            geom = from_shape(geom_shape, srid=4326)

            data = data_set.read(1, masked=True, out_dtype=numpy.uint8)
            data[data == numpy.ma.masked] = 0
            data[data != numpy.ma.masked] = 1
            geoms = []
            for _geom, _ in rasterio.features.shapes(data, mask=data, transform=data_set.transform):
                _geom = rasterio.warp.transform_geom(data_set.crs, 'EPSG:4326', _geom, precision=6)

                geoms.append(shapely.geometry.shape(geom))

            # TODO: Simplify geometries
            if len(geoms) == 1:
                min_convex_hull = geoms[0].convex_hull
            else:
                min_convex_hull = shapely.geometry.MultiPolygon(geoms).convex_hull

            chunk_x, chunk_y = data_set.profile.get('blockxsize'), data_set.profile.get('blockxsize')

            if chunk_x and chunk_x:
                asset['bdc:chunk_size'] = dict(x=chunk_x, y=chunk_y)
    
    return asset, geom, min_convex_hull
