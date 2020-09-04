#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import datetime
from typing import List

import numpy
import rasterio
import hashlib
import os
from dateutil.relativedelta import relativedelta
from numpngw import write_png
from rasterio.io import MemoryFile
from rasterio.warp import Resampling


#############################
def get_date(str_date):
    return datetime.datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')


#############################
def days_in_month(date):
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    nday = int(date.split('-')[2])
    if month == 12:
        nmonth = 1
        nyear = year +1
    else:
        nmonth = month + 1
        nyear = year
    ndate = '{0:4d}-{1:02d}-{2:02d}'.format(nyear,nmonth,nday)
    td = numpy.datetime64(ndate) - numpy.datetime64(date)
    return td


#############################
def decode_periods(temporal_schema, start_date, end_date, time_step):
    """
    Retrieve datacube temporal resolution by periods.
    """
    requested_periods = {}
    if start_date is None:
        return requested_periods
    if isinstance(start_date, datetime.date):
        start_date = start_date.strftime('%Y-%m-%d')

    td_time_step = datetime.timedelta(days=time_step)
    steps_per_period = int(round(365./time_step))

    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    if isinstance(end_date, datetime.date):
        end_date = end_date.strftime('%Y-%m-%d')

    if temporal_schema is None:
        periodkey = start_date + '_' + start_date + '_' + end_date
        requested_period = list()
        requested_period.append(periodkey)
        requested_periods[start_date] = requested_period
        return requested_periods

    if temporal_schema == 'M':
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        delta = relativedelta(months=time_step)
        requested_period = []
        while start_date <= end_date:
            next_date = start_date + delta
            periodkey = str(start_date)[:10] + '_' + str(start_date)[:10] + '_' + str(next_date - relativedelta(days=1))[:10]
            requested_period.append(periodkey)
            requested_periods[start_date] = requested_period
            start_date = next_date
        return requested_periods

    # Find the exact start_date based on periods that start on yyyy-01-01
    firstyear = start_date.split('-')[0]
    new_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    if temporal_schema == 'A':
        dbase = datetime.datetime.strptime(firstyear+'-01-01', '%Y-%m-%d')
        while dbase < new_start_date:
            dbase += td_time_step
        if dbase > new_start_date:
            dbase -= td_time_step
        start_date = dbase.strftime('%Y-%m-%d')
        new_start_date = dbase

    # Find the exact end_date based on periods that start on yyyy-01-01
    lastyear = end_date.split('-')[0]
    new_end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    if temporal_schema == 'A':
        dbase = datetime.datetime.strptime(lastyear+'-12-31', '%Y-%m-%d')
        while dbase > new_end_date:
            dbase -= td_time_step
        end_date = dbase
        if end_date == start_date:
            end_date += td_time_step - datetime.timedelta(days=1)
        end_date = end_date.strftime('%Y-%m-%d')

    # For annual periods
    if temporal_schema == 'A':
        dbase = new_start_date
        yearold = dbase.year
        count = 0
        requested_period = []
        while dbase < new_end_date:
            if yearold != dbase.year:
                dbase = datetime.datetime(dbase.year,1,1)
            yearold = dbase.year
            dstart = dbase
            dend = dbase + td_time_step - datetime.timedelta(days=1)
            dend = min(datetime.datetime(dbase.year, 12, 31), dend)
            basedate = dbase.strftime('%Y-%m-%d')
            start_date = dstart.strftime('%Y-%m-%d')
            end_date = dend.strftime('%Y-%m-%d')
            periodkey = basedate + '_' + start_date + '_' + end_date
            if count % steps_per_period == 0:
                count = 0
                requested_period = []
                requested_periods[basedate] = requested_period
            requested_period.append(periodkey)
            count += 1
            dbase += td_time_step
        if len(requested_periods) == 0 and count > 0:
            requested_periods[basedate].append(requested_period)
    else:
        yeari = start_date.year
        yearf = end_date.year
        monthi = start_date.month
        monthf = end_date.month
        dayi = start_date.day
        dayf = end_date.day
        for year in range(yeari,yearf+1):
            dbase = datetime.datetime(year,monthi,dayi)
            if monthi <= monthf:
                dbasen = datetime.datetime(year,monthf,dayf)
            else:
                dbasen = datetime.datetime(year+1,monthf,dayf)
            while dbase < dbasen:
                dstart = dbase
                dend = dbase + td_time_step - datetime.timedelta(days=1)
                basedate = dbase.strftime('%Y-%m-%d')
                start_date = dstart.strftime('%Y-%m-%d')
                end_date = dend.strftime('%Y-%m-%d')
                periodkey = basedate + '_' + start_date + '_' + end_date
                requested_period = []
                requested_periods[basedate] = requested_period
                requested_periods[basedate].append(periodkey)
                dbase += td_time_step
    return requested_periods


#############################
def encode_key(activity, keylist):
    dynamoKey = ''
    for key in keylist:
        dynamoKey += activity[key]
    return dynamoKey


############################
def qa_statistics(raster):
    """Retrieve raster statistics efficacy and cloud ratio, based in Fmask values.
    Notes:
        Values 0 and 1 are considered `clear data`.
    """
    totpix = raster.size
    clearpix = numpy.count_nonzero(raster < 2)
    cloudpix = numpy.count_nonzero(raster > 1)
    imagearea = clearpix + cloudpix
    cloudratio = 100

    if imagearea != 0:
        cloudratio = round(100.*cloudpix/imagearea, 1)
    efficacy = round(100.*clearpix/totpix, 2)

    return efficacy, cloudratio

############################
def getMask(raster, satellite):
    """Retrieves and re-sample quality raster to well-known values used in Brazil Data Cube.

    We adopted the `Fmask <https://github.com/GERSL/Fmask>`_ (Function of Mask).
    TODO: Add paper/authors reference

    In the Fmask output product, the following classes are presented in a normative quality:

    - 0: Clear Land Pixel
    - 1: Clear Water Pixel
    - 2: Cloud Shadow
    - 3: Snow-ice
    - 4: Cloud
    - 255: no observation

    For satellite which does not supports these values, consider to expose individual values.
    For example:
        CBERS does not have `Snow-ice`, `Water` or `Cloud Shadow` pixel values. The following values are described
        in CBERS quality band:
        - `0`: Fill/Nodata. Re-sample to `No observation` (255);
        - `127: Valid Data. Re-sample to `Clear Land Pixel` (0);
        - `255`: Cloudy. Re-sample to `Cloud` (4)

    Args:
        raster - Raster with Quality band values
        satellite - Satellite Type

    Returns:
        Tuple containing formatted quality raster, efficacy and cloud ratio, respectively
    """
    rastercm = raster
    if satellite == 'MODIS':
        # MOD13Q1 Pixel Reliability !!!!!!!!!!!!!!!!!!!!
        # Note that 1 was added to this image in downloadModis because of warping
        # Rank/Key Summary QA 		Description
        # -1 		Fill/No Data 	Not Processed
        # 0 		Good Data 		Use with confidence
        # 1 		Marginal data 	Useful, but look at other QA information
        # 2 		Snow/Ice 		Target covered with snow/ice
        # 3 		Cloudy 			Target not visible, covered with cloud
        lut = numpy.array([255, 0, 0, 2, 4], dtype=numpy.uint8)
        rastercm = numpy.take(lut, raster+1).astype(numpy.uint8)

    elif 'CBERS' in satellite:
        # Key Summary        QA Description
        #   0 Fill/No Data - Not Processed
        # 127 Good Data    - Use with confidence
        # 255 Cloudy       - Target not visible, covered with cloud
        # fill = 0  # warped images have 0 as fill area
        lut = numpy.zeros(256, dtype=numpy.uint8)
        lut[0] = 255
        lut[127] = 0
        lut[255] = 4
        rastercm = numpy.take(lut, raster).astype(numpy.uint8)

    efficacy, cloudratio = qa_statistics(rastercm)

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
def get_cube_id(cube, function=None):
    if not function or function.upper() == 'IDENTITY':
        return '_'.join(cube.split('_')[:-1])
    else:
        return '{}_{}'.format(cube, function)


def get_cube_parts(datacube: str) -> List[str]:
    """Parse a data cube name and retrieve their parts.

    A data cube is composed by the following structure:
    ``Collections_Resolution_TemporalPeriod_CompositeFunction``.

    An IDENTITY data cube does not have TemporalPeriod and CompositeFunction.

    Examples:
        >>> # Parse Sentinel 2 Monthly MEDIAN
        >>> get_cube_parts('S2_10_1M_MED') # ['S2', '10', '1M', 'MED']
        >>> # Parse Sentinel 2 IDENTITY
        >>> get_cube_parts('S2_10') # ['S2', '10']
        >>> # Invalid data cubes
        >>> get_cube_parts('S2-10')

    Raises:
        ValueError when data cube name is invalid.
    """
    cube_fragments = datacube.split('_')

    if len(cube_fragments) > 4 or len(cube_fragments) < 2:
        raise ValueError('Invalid data cube name. "{}"'.format(datacube))

    return cube_fragments


#############################
def get_resolution_by_satellite(satellite):
    resolutions = {
        'CBERS-4-MUX': '20',
        'CBERS-4-WFI': '64',
        'MODIS': '231',
        'LANDSAT': '30',
        'SENTINEL-2': '10',
    }
    return resolutions[satellite]


#############################
def revisit_by_satellite(satellite):
    resolutions = {
        'CBERS-4-MUX': 26,
        'CBERS-4-WFI': 6,
        'MODIS': 2,
        'LANDSAT': 16,
        'SENTINEL-2': 5,
    }
    return resolutions[satellite]


############################
def generate_hash_md5(word):
    result = hashlib.md5(word.encode())
    return result.hexdigest()


############################
def create_cog_in_s3(services, profile, path, raster, is_quality, nodata, bucket_name):
    profile.update({
        'compress': 'LZW',
        'tiled': True,
        'interleave': 'pixel',
        'blockxsize': 256,
        'blockysize': 256
    })

    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            if is_quality:
                mem.nodata = nodata
            
            mem.write_band(1, raster)
            mem.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
            mem.update_tags(ns='rio_overview', resampling='nearest')

        services.upload_fileobj_S3(memfile, path, {'ACL': 'public-read'}, bucket_name=bucket_name)
    return True


############################
def create_index(services, index, bands, bucket_name):
    prefix = services.get_s3_prefix(bucket_name)

    red_band_name = bands['red'] if bands.get('red') else bands['RED']
    red_band_path = os.path.join(prefix + red_band_name)
    nir_band_name = bands['nir'] if bands.get('nir') else bands['NIR']
    nir_band_path = os.path.join(prefix + nir_band_name)

    raster = None

    if index.upper() == 'EVI':
        blue_bland_path = bands['blue'] if bands.get('blue') else bands['BLUE']
        blue_bland_path = os.path.join(prefix + blue_bland_path) 

        with rasterio.open(nir_band_path) as ds_nir, rasterio.open(red_band_path) as ds_red,\
            rasterio.open(blue_bland_path) as ds_blue:

            profile = ds_nir.profile
            nodata = int(profile['nodata'])
            blocks = ds_nir.block_windows()
            data_type = profile['dtype']

            raster = numpy.full((profile['height'], profile['width']), dtype=data_type, fill_value=nodata)

            for _, block in blocks:
                row_offset = block.row_off + block.height
                col_offset = block.col_off + block.width

                nir = ds_nir.read(1, masked=True, window=block)
                red = ds_red.read(1, masked=True, window=block)
                blue = ds_blue.read(1, masked=True, window=block)

                # Calculate EVI
                raster_block = (10000. * 2.5 * (nir - red) / (nir + 6. * red - 7.5 * blue + 10000.)).astype(numpy.int16)
                raster_block[raster_block == numpy.ma.masked] = nodata
                raster[block.row_off: row_offset, block.col_off: col_offset] = raster_block

    if index.upper() == 'NDVI':
        with rasterio.open(nir_band_path) as ds_nir, rasterio.open(red_band_path) as ds_red:
            profile = ds_nir.profile
            nodata = int(profile['nodata'])
            blocks = ds_nir.block_windows()
            data_type = profile['dtype']

            raster = numpy.full((profile['height'], profile['width']), dtype=data_type, fill_value=nodata)

            for _, block in blocks:
                row_offset = block.row_off + block.height
                col_offset = block.col_off + block.width

                nir = ds_nir.read(1, masked=True, window=block)
                red = ds_red.read(1, masked=True, window=block)

                # Calculate NDVI
                raster_block = (10000. * ((nir - red) / (nir + red))).astype(numpy.int16)
                raster_block[raster_block == numpy.ma.masked] = nodata
                raster[block.row_off: row_offset, block.col_off: col_offset] = raster_block

    file_path = '_'.join(red_band_name.split('_')[:-1]) + '_{}.tif'.format(index)                
    create_cog_in_s3(
        services, profile, file_path, raster.astype(numpy.int16), False, None, bucket_name)