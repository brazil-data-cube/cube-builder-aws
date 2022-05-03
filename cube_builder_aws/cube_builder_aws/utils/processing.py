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
import shutil
from collections import Iterable
from pathlib import Path
from typing import List, NamedTuple, Tuple, Union

import numpy
import rasterio
import rasterio.features
import rasterio.warp
import shapely.geometry
from bdc_catalog.models import db
from bdc_catalog.utils import \
    multihash_checksum_sha256 as _multihash_checksum_sha256
from flask import abort
from geoalchemy2.shape import from_shape
from numpngw import write_png
from rasterio.io import MemoryFile
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from ..logger import logger
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
def extract_qa_bits(band_data, bit_location):
    """Get bit information from given position.
    Args:
        band_data (numpy.ma.masked_array) - The QA Raster Data
        bit_location (int) - The band bit value
    """
    return band_data & (1 << bit_location)


NO_CONFIDENCE = 0
LOW = 1  # 0b01
MEDIUM = RESERVED = 2  # 0b10
HIGH = 3  # 0b11


class QAConfidence:
    """Type for Quality Assessment definition for Landsat Collection 2.
    These properties will be evaluated using Python Virtual Machine like::
        # Define that will discard all cloud values which has confidence greater or equal MEDIUM.
        qa = QAConfidence(cloud='cloud >= MEDIUM', cloud_shadow=None, cirrus=None, snow=None)
    """

    cloud: Union[str, None]
    """Represent the Cloud Confidence."""
    cloud_shadow: Union[str, None]
    """Represent the Cloud Shadow Confidence."""
    cirrus: Union[str, None]
    """Represent the Cirrus."""
    snow: Union[str, None]
    """Represent the Snow/Ice."""
    landsat_8: bool
    """Flag to identify Landsat-8 Satellite."""

    def __init__(self, cloud=None, cloud_shadow=None, cirrus=None, snow=None, landsat_8=None):
        self.cloud = cloud
        self.cloud_shadow = cloud_shadow
        self.cirrus = cirrus
        self.snow = snow
        self.landsat_8 = landsat_8


def qa_cloud_confidence(data, confidence: QAConfidence):
    """Apply the Bit confidence to the Quality Assessment mask."""
    from .interpreter import execute

    # Define the context variables available for cloud confidence processing
    ctx = dict(
        NO_CONFIDENCE=NO_CONFIDENCE,
        LOW=LOW,
        MEDIUM=MEDIUM,
        RESERVED=RESERVED,
        HIGH=HIGH
    )

    def _invoke(conf, context_var, start_offset, end_offset, qa):
        var_name = f'_{context_var}'
        expression = f'{var_name} = {conf}'
        array = (qa >> start_offset) - ((qa >> end_offset) << 2)
        ctx[context_var] = array

        _res = execute(expression, context=ctx)
        res = _res[var_name]

        return numpy.ma.masked_where(numpy.ma.getdata(res), qa)

    if confidence.cloud:
        data = _invoke(confidence.cloud, 'cloud', 8, 10, data)
    if confidence.cloud_shadow:
        data = _invoke(confidence.cloud_shadow, 'cloud_shadow', 10, 12, data)
    if confidence.snow:
        data = _invoke(confidence.snow, 'snow', 12, 14, data)

    if confidence.cirrus:
        if isinstance(confidence.landsat_8, bool):
            confidence.landsat_8 = numpy.full(data.shape,
                                              dtype=numpy.bool_,
                                              fill_value=confidence.landsat_8)

        landsat_8_pixels = data[confidence.landsat_8]
        res = _invoke(confidence.cirrus, 'cirrus', 14, 16, landsat_8_pixels)

        data[confidence.landsat_8] = res

    return data


def get_qa_mask(data: numpy.ma.masked_array,
                clear_data: List[float] = None,
                not_clear_data: List[float] = None,
                nodata: float = None,
                confidence: QAConfidence = None) -> numpy.ma.masked_array:
    """Extract Quality Assessment Bits from Landsat-8 Collection 2 Level-2 products.

    This method uses the bitwise operation to extract bits according to the document
    `Landsat 8 Collection 2 (C2) Level 2 Science Product (L2SP) Guide <https://prd-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/atoms/files/LSDS-1619_Landsat8-C2-L2-ScienceProductGuide-v2.pdf>`_, page 13.

    Example:
        >>> import numpy
        >>> from cube_builder.utils.image import QAConfidence, get_qa_mask

        >>> mid_cloud_confidence = QAConfidence(cloud='cloud == MEDIUM', cloud_shadow=None, cirrus=None, snow=None, landsat_8=True)
        >>> clear = [6, 7]  # Clear and Water
        >>> not_clear = [1, 2, 3, 4]  # Dilated Cloud, Cirrus, Cloud, Cloud Shadow
        >>> get_qa_mask(numpy.ma.array([22080], dtype=numpy.int16, fill_value=1),
        ...             clear_data=clear, not_clear_data=not_clear,
        ...             nodata=1, confidence=mid_cloud_confidence)
        masked_array(data=[--],
                     mask=[ True],
               fill_value=1,
                    dtype=int16)
        >>> # When no cloud confidence set, this value will be Clear since Cloud Pixel is off.
        >>> get_qa_mask(numpy.ma.array([22080], dtype=numpy.int16, fill_value=1),
        ...             clear_data=clear, not_clear_data=not_clear,
        ...             nodata=1)
        masked_array(data=[22080],
                     mask=[False],
               fill_value=1,
                    dtype=int16)

    Args:
        data (numpy.ma.masked_array): The QA Raster Data
        clear_data (List[float]): The bits values to be considered as Clear. Default is [].
        not_clear_data (List[float]): The bits values to be considered as Not Clear Values (Cloud,Shadow, etc).
        nodata (float): Pixel nodata value.
        confidence (QAConfidence): The confidence rules mapping. See more in :class:`~cube_builder.utils.image.QAConfidence`.

    Returns:
        numpy.ma.masked_array: An array which the values represents `clear_data` and the masked values represents `not_clear_data`.
    """
    is_numpy_or_masked_array = type(data) in (numpy.ndarray, numpy.ma.masked_array)
    if type(data) in (float, int,):
        data = numpy.ma.masked_array([data])
    elif (isinstance(data, Iterable) and not is_numpy_or_masked_array) or (isinstance(data, numpy.ndarray) and not hasattr(data, 'mask')):
        data = numpy.ma.masked_array(data, mask=data == nodata, fill_value=nodata)
    elif not is_numpy_or_masked_array:
        raise TypeError(f'Expected a number or numpy masked array for {data}')

    if nodata is not None:
        data[data == nodata] = numpy.ma.masked

    # Cloud Confidence only once
    if confidence:
        data = qa_cloud_confidence(data, confidence=confidence)

    # Mask all not clear data before get any valid data
    for value in not_clear_data:
        if value == 2 and confidence:
            not_landsat_8_positions = numpy.where(numpy.invert(confidence.landsat_8))
            tmp_result = numpy.ma.masked_where(data.mask, data)
            tmp_result.mask[not_landsat_8_positions] = True

            masked = extract_qa_bits(tmp_result, value)
            if isinstance(masked.mask, numpy.bool_):
                masked.mask = numpy.full(data.shape, fill_value=masked.mask, dtype=numpy.bool_)
                
            masked.mask[not_landsat_8_positions] = data[not_landsat_8_positions].mask

            tmp_result = None
        else:
            masked = extract_qa_bits(data, value)

        data = numpy.ma.masked_where(masked > 0, data)

    clear_mask = data.mask.copy()
    for value in clear_data:
        masked = numpy.ma.getdata(extract_qa_bits(data, value))
        clear_mask = numpy.ma.logical_or(masked > 0, clear_mask)

    if len(data.mask.shape) > 0:
        data = numpy.ma.masked_where(numpy.invert(clear_mask), data)
    else:  # Adapt to work with single value
        if clear_mask[0]:
            data.mask = numpy.invert(clear_mask)

    return data

############################
def qa_statistics(raster, mask, blocks, confidence=None) -> Tuple[float, float]:
    """Retrieve raster statistics efficacy and not clear ratio, based in Fmask values.

    Notes:
        Values 0 and 1 are considered `clear data`.
        Values 2 and 4 are considered as `not clear data`
        The values for snow `3` and nodata `255` is not used to count efficacy and not clear ratio
    """
    # Compute how much data is for each class. It will be used as image area
    clear_pixels = 0
    not_clear_pixels = 0

    data_masked = numpy.ma.masked_array(raster, mask=raster == mask['nodata'], fill_value=mask['nodata'])

    for _, window in blocks:
        row_offset = window.row_off + window.height
        col_offset = window.col_off + window.width

        raster_block = raster[window.row_off: row_offset, window.col_off: col_offset]

        if mask.get('bits'):
            nodata_pixels = raster[raster == mask['nodata']].size
            qa_mask = get_qa_mask(data_masked, clear_data=mask['clear_data'], 
                                    not_clear_data=mask['not_clear_data'], nodata=mask['nodata'],
                                    confidence=confidence)
            # Since the nodata values is already masked, we should remove the difference
            not_clear_pixels = qa_mask[qa_mask.mask].size - nodata_pixels
            clear_pixels = qa_mask[numpy.invert(qa_mask.mask)].size
        else:
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
def generateQLook(generalSceneId, qlfiles):
    try:
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
        
    except Exception as e:
        logger.error(str(e), exc_info=True)

        raise e


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
def create_cog_in_s3(services, profile, path, raster, bucket_name, nodata=None, tags=None):
    with MemoryFile() as dst_file:
        with MemoryFile() as memfile:
            with memfile.open(**profile) as mem:
                if nodata:
                    mem.nodata = nodata
                mem.write_band(1, raster)

                if tags:
                    mem.update_tags(**tags)
                
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
def create_index(services, index, bands_expressions, bands, bucket_name, index_file_path):
    """Generate data cube custom bands based in string-expression.

    This method seeks for custom bands on Collection Band definition. A custom band must have
    `metadata` property filled out according the ``bdc_catalog.jsonschemas.band-metadata.json``.

    Args:
        services - AWS service wrapper
        index - The band name
        bands_expressions - Map of band expressions
        bands - Map of data cube bands path
        bucket_name - Bucket name where the data cube is stored
        index_file_path - Path index path.

    Raises:
        RuntimeError when an error occurs while interpreting the band expression in Python Virtual Machine.

    Returns:
        A dict values with generated bands.
    """
    prefix = services.get_s3_prefix(bucket_name)

    map_data_set_context = dict()
    band_definition = bands_expressions[index]
    band_expression = band_definition['expression']['value']
    band_data_type = band_definition['data_type']
    data_type_info = numpy.iinfo(band_data_type)
    data_type_max_value = data_type_info.max
    data_type_min_value = data_type_info.min

    profile = blocks = None

    for _band, relative_path in bands.items():
        absolute_path = os.path.join(prefix, str(relative_path))
        map_data_set_context[_band] = AutoCloseDataSet(absolute_path)

        if profile is None:
            profile = map_data_set_context[_band].dataset.profile
            blocks = map_data_set_context[_band].dataset.block_windows()

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
        # Persist the expected band data type to cast value safely.
        # TODO: Should we use consider band min_value/max_value?
        raster_block[raster_block < data_type_min_value] = data_type_min_value
        raster_block[raster_block > data_type_max_value] = data_type_max_value

        raster[window.row_off: row_offset, window.col_off: col_offset] = raster_block

    create_cog_in_s3(services, profile, index_file_path, raster.astype(numpy.int16), bucket_name)


############################
def format_version(version, prefix='v'):
    return f'{prefix}{version}'


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
    try:
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
                geom = from_shape(geom_shape, srid=4326, extended=True)

                # data = data_set.read(1, masked=True, out_dtype=numpy.uint8)
                # data[data == numpy.ma.masked] = 0
                # data[data != numpy.ma.masked] = 1
                # geoms = []
                # for _geom, _ in rasterio.features.shapes(data, mask=data, transform=data_set.transform):
                #     _geom = rasterio.warp.transform_geom(data_set.crs, 'EPSG:4326', _geom, precision=6)

                #     geoms.append(shapely.geometry.shape(geom))

                # TODO: Simplify geometries
                # if len(geoms) == 1:
                #     min_convex_hull = geoms[0].convex_hull
                # else:
                #     min_convex_hull = shapely.geometry.MultiPolygon(geoms).convex_hull

                chunk_x, chunk_y = data_set.profile.get('blockxsize'), data_set.profile.get('blockxsize')

                if chunk_x and chunk_y:
                    asset['bdc:chunk_size'] = dict(x=chunk_x, y=chunk_y)

        return asset, geom, min_convex_hull

    except Exception as e:
        logger.error(str(e), exc_info=True)

        raise e

def apply_landsat_harmonization(services, url, band, angle_bucket_dir=None, quality_band=False):
    """Generate Landsat NBAR.

    Args:
        url (str) - link scene
        band (str) - band name
        angle_bucket_dir Optional[str] - path to directory/bucket containing angle bands.

    Returns:
        str: full path result images.
    """
    from sensor_harm.landsat import landsat_harmonize

    try:
        scene_id = Path(url).stem.replace(f'_{band}', '')

        # download scene to temp directory
        source_dir = f'/tmp/processing/{scene_id}' if not quality_band else '/tmp/processing/quality'
        Path(source_dir).mkdir(parents=True, exist_ok=True)
        _ = download_raster_aws(services, url, dst_path=f'{source_dir}/{Path(url).name}', requester_pays=True)

        if quality_band:
            return f'{source_dir}/{Path(url).name}'
            
        # download angs to temp directory
        url_parts = url.replace('s3://', '').split('/')
        angle_folder = '/'.join(url_parts[2:-1])
        for angle_key in ['sensor_azimuth', 'sensor_zenith', 'solar_azimuth', 'solar_zenith']:
            angle_file = f'{scene_id}_{angle_key}_B04.tif'
            path = f'{angle_bucket_dir}/{angle_folder}/{angle_file}'
            _ = download_raster_aws(services, path, dst_path=f'{source_dir}/{angle_file}', requester_pays=True)

        target_dir = '/tmp/processing/result'
        result_path = url

        _, result_paths = landsat_harmonize(scene_id, source_dir, target_dir, bands=[band], cp_quality_band=False)
        for r in result_paths:
            if r.get(band, None):
                result_path = r[band]

        shutil.rmtree(source_dir)

        return str(result_path)

    except Exception:
        raise
        

def download_raster_aws(services, path, dst_path, requester_pays=False):
    from rasterio.session import AWSSession

    aws_session = AWSSession(services.session, requester_pays=requester_pays)
    with rasterio.Env(aws_session, AWS_SESSION_TOKEN=""):
        with rasterio.open(path) as src:
            profile = src.profile.copy()
            profile.update({
                'blockxsize': 2048,
                'blockysize': 2048,
                'tiled': True
            })
            arr = src.read(1)

            with rasterio.open(dst_path, 'w', **profile) as dataset:
                dataset.write(arr, 1)

    return dst_path


def get_item_name(cube_name: str, version: str, tile: str, date: str) -> str:
    return f'{cube_name}_{version}_{tile}_{date}'.upper()


def get_cube_path(cube_name: str, version: str, tile: str, date: str) -> str:
    cube_date = format_date_path(date)
    horizontal = tile[:3]
    vertical = tile[-3:]
    return f'{cube_name.lower()}/{version}/{horizontal}/{vertical}/{cube_date}'


def format_date_path(date: str) -> str:
    cube_date = datetime.datetime.strptime(date, '%Y-%m-%d')
    year = cube_date.strftime("%Y")
    month = cube_date.strftime("%m")
    day = cube_date.strftime("%d")

    return f'{year}/{month}/{day}'
