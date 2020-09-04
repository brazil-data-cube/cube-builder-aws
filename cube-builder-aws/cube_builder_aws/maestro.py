#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import json
import os
import numpy
import rasterio

from datetime import datetime
from pathlib import Path
from geoalchemy2 import func
from rasterio.transform import Affine 
from rasterio.warp import reproject, Resampling, transform
from rasterio.merge import merge 
from rasterio.io import MemoryFile

from bdc_catalog.models.base_sql import BaseModel, db
from bdc_catalog.models import Collection, Tile, GridRefSys, Item, Band

from .logger import logger
from .utils.constants import RESOLUTION_BY_SATELLITE, COG_MIME_TYPE
from .utils.builder import decode_periods, encode_key, \
    qa_statistics, getMask, generateQLook, get_cube_name, \
    create_cog_in_s3, create_index, format_version, create_asset_definition

def orchestrate(cube_infos, tiles, start_date, end_date, functions):
    formatted_version = format_version(cube_infos.version)

    tiles_by_grs = db.session() \
        .query(Tile, GridRefSys) \
        .filter(
            Tile.grid_ref_sys_id == cube_infos.grid_ref_sys_id,
            Tile.name.in_(tiles),
            GridRefSys.id == Tile.grid_ref_sys_id
        ).all()

    collection_tiles = []
    tiles_infos = []
    for tile in tiles_by_grs:
        grid_table = tile.GridRefSys.geom_table
        
        tile_stats = db.session.query(
            (func.ST_XMin(grid_table.c.geom)).label('min_x'),
            (func.ST_YMax(grid_table.c.geom)).label('max_y'),
            (func.ST_XMax(grid_table.c.geom) - func.ST_XMin(grid_table.c.geom)).label('dist_x'),
            (func.ST_YMax(grid_table.c.geom) - func.ST_YMin(grid_table.c.geom)).label('dist_y'),
            (func.ST_AsText(func.ST_BoundingDiagonal(func.ST_Transform(grid_table.c.geom, 4326)))).label('bbox')
        ).filter(
            grid_table.c.tile == tile.Tile.name
        ).first()

        tiles_infos.append(dict(
            id=tile.Tile.id,
            name=tile.Tile.name,
            stats=tile_stats
        ))

    # get cube start_date if exists
    cube_start_date = cube_infos.start_date or start_date

    # get/mount timeline
    # TODO: review code (decode_periods)
    temporal_schema = 'A' if  cube_infos.temporal_composition_schema['schema'] != 'month' else 'M'
    step = cube_infos.temporal_composition_schema['step']
    timeline = decode_periods(temporal_schema.upper(), cube_start_date, end_date, int(step))

    # get items
    collections_items = Item.query().filter(Item.collection_id == cube_infos.id).all()

    # create collection items (old model => mosaic)
    items_id = []
    items = {}
    for datekey in sorted(timeline):
        requestedperiod = timeline[datekey]
        for periodkey in requestedperiod:
            (p_basedate, p_startdate, p_enddate) = periodkey.split('_')

            if start_date is not None and p_startdate < start_date : continue
            if end_date is not None and p_enddate > end_date : continue

            for tile in tiles_infos:
                tile_id = tile['id']
                tile_name = tile['name']
                tile_stats = tile['stats']
                bbox = tile_stats.bbox
                bbox_formated = bbox[bbox.find('(') + 1:bbox.find(')')].replace(' ', ',')

                items[tile_name] = items.get(tile_name, {})
                items[tile_name]['bbox'] = bbox_formated
                items[tile_name]['xmin'] = tile_stats.min_x
                items[tile_name]['ymax'] = tile_stats.max_y
                items[tile_name]['dist_x'] = tile_stats.dist_x
                items[tile_name]['dist_y'] = tile_stats.dist_y
                items[tile_name]['periods'] = items[tile_name].get('periods', {})

                item_id = f'{cube_infos.name}_{tile_name}_{p_basedate}'
                if item_id not in items_id:
                    items_id.append(item_id)
                    items[tile_name]['periods'][periodkey] = {
                        'collection': cube_infos.name,
                        'version': cube_infos.version,
                        'collection_id': cube_infos.id,
                        'tile_id': tile_id,
                        'tile_name': tile_name,
                        'item_date': p_basedate,
                        'id': item_id,
                        'composite_start': p_startdate,
                        'composite_end': p_enddate,
                        'dirname': f'{cube_infos.name}/{formatted_version}/{tile_name}/'
                    }
    return items


def solo(self, activitylist):
    services = self.services

    for activity in activitylist:
        services.put_activity(activity)
        if activity['mystatus'] == 'DONE' and activity['action'] != 'publish':
            next_step(services, activity)


def next_step(services, activity):
    activitiesControlTableKey = activity['dynamoKey']\
            .replace(activity['band'], '')
    if activity['action'] == 'merge':
        activitiesControlTableKey = activitiesControlTableKey.replace(
            activity['date'], '{}{}'.format(activity['start'], activity['end']))

    response = services.update_control_table(
        Key = {'id': activitiesControlTableKey},
        UpdateExpression = "ADD #mycount :increment",
        ExpressionAttributeNames = {'#mycount': 'mycount'},
        ExpressionAttributeValues = {':increment': 1},
        ReturnValues = "UPDATED_NEW"
    )
    if 'Attributes' in response and 'mycount' in response['Attributes']:
        mycount = int(response['Attributes']['mycount'])
        if mycount >= activity['totalInstancesToBeDone']:
            if activity['action'] == 'merge':
                next_blend(services, activity)
            elif activity['action'] == 'blend':
                if activity.get('indexes') and len(activity['indexes']) > 0:
                    next_posblend(services, activity)
                else:
                    next_publish(services, activity)
            elif activity['action'] == 'posblend':
                next_publish(services, activity)


###############################
# MERGE
###############################
def prepare_merge(self, datacube, datasets, satellite, bands, indexes, quicklook, resx,
                  resy, nodata, crs, quality_band, functions, version, force=False):
    services = self.services

    # Build the basics of the merge activity
    activity = {}
    activity['action'] = 'merge'
    activity['datacube_orig_name'] = datacube
    activity['datacube'] = get_cube_name(datacube)
    activity['version'] = version
    activity['datasets'] = datasets
    activity['satellite'] = satellite.upper()
    activity['bands'] = bands
    activity['indexes'] = indexes
    activity['quicklook'] = quicklook
    activity['resx'] = resx
    activity['resy'] = resy
    activity['nodata'] = nodata
    activity['srs'] = crs
    activity['bucket_name'] = services.bucket_name
    activity['url_stac'] = services.url_stac
    activity['quality_band'] = quality_band
    activity['functions'] = functions
    activity['force'] = force
    activity['internal_bands'] = ['CLEAROB', 'TOTALOB', 'PROVENANCE']

    logger.info('prepare merge - Score {} items'.format(self.score['items']))

    # For all tiles
    for tile_name in self.score['items']:
        activity['tileid'] = tile_name
        activity['tileid_original'] = tile_name

        # GET bounding box - tile ID
        activity['bbox'] = self.score['items'][tile_name]['bbox']
        activity['xmin'] = self.score['items'][tile_name]['xmin']
        activity['ymax'] = self.score['items'][tile_name]['ymax']
        activity['dist_x'] = self.score['items'][tile_name]['dist_x']
        activity['dist_y'] = self.score['items'][tile_name]['dist_y']

        # For all periods
        for periodkey in self.score['items'][tile_name]['periods']:
            activity['start'] = self.score['items'][tile_name]['periods'][periodkey]['composite_start']
            activity['end'] = self.score['items'][tile_name]['periods'][periodkey]['composite_end']
            activity['dirname'] = self.score['items'][tile_name]['periods'][periodkey]['dirname']

            # When force is True, we must rebuild the merge
            if force:
                merge_control_key = encode_key(activity, ['action', 'datacube', 'tileid', 'start', 'end'])
                blend_control_key = 'blend{}_1M{}'.format(activity['datacube'],
                                                          encode_key(activity, ['tileid', 'start', 'end']))
                self.services.remove_control_by_key(merge_control_key)
                self.services.remove_control_by_key(blend_control_key)

            # Search all images
            self.score['items'][tile_name]['periods'][periodkey]['scenes'] = services.search_STAC(activity)

            # Evaluate the number of dates, the number of scenes for each date and
            # the total amount merges that will be done
            number_of_datasets_dates = 0
            first_band = list(self.score['items'][tile_name]['periods'][periodkey]['scenes'].keys())[0]
            list_dates = []
            for dataset in self.score['items'][tile_name]['periods'][periodkey]['scenes'][first_band].keys():
                for date in self.score['items'][tile_name]['periods'][periodkey]['scenes'][first_band][dataset].keys():
                    list_dates.append(str(date)[:10])
                    number_of_datasets_dates += 1
            activity['instancesToBeDone'] = number_of_datasets_dates
            activity['totalInstancesToBeDone'] = number_of_datasets_dates * len(activity['bands'])

            # Reset mycount in activitiesControlTable
            activities_control_table_key = encode_key(activity, ['action','datacube','tileid','start','end'])
            services.put_control_table(activities_control_table_key, 0)

            # Save the activity in DynamoDB if no scenes are available
            if number_of_datasets_dates == 0:
                dynamo_key = encode_key(activity, ['action','datacube','tileid','start','end'])
                activity['dynamoKey'] = dynamo_key
                activity['sk'] = 'NOSCENES'
                activity['mystatus'] = 'ERROR'
                activity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                activity['mystart'] = 'XXXX-XX-XX'
                activity['myend'] = 'YYYY-YY-YY'
                activity['efficacy'] = '0'
                activity['cloudratio'] = '100'
                services.put_item_kinesis(activity)
                continue

            # Build each merge activity
            # For all bands
            activity['list_dates'] = list_dates
            for band in self.score['items'][tile_name]['periods'][periodkey]['scenes']:
                activity['band'] = band

                # For all datasets
                for dataset in self.score['items'][tile_name]['periods'][periodkey]['scenes'][band]:
                    activity['dataset'] = dataset

                    # get resolution by satellite
                    activity['resolution'] = RESOLUTION_BY_SATELLITE.get(activity['satellite'])
                    # For all dates
                    for date in self.score['items'][tile_name]['periods'][periodkey]['scenes'][band][dataset]:
                        activity['date'] = date[0:10]
                        activity['links'] = []

                        # Create the dynamoKey for the activity in DynamoDB
                        activity['dynamoKey'] = encode_key(activity, ['action','datacube','tileid','date','band'])

                        # Get all scenes that were acquired in the same date
                        for scene in self.score['items'][tile_name]['periods'][periodkey]['scenes'][band][dataset][date]:
                            activity['links'].append(scene['link'])

                        # Continue filling the activity
                        activity['ARDfile'] = activity['dirname']+'{}/{}_{}_{}_{}_{}.tif'.format(date[0:10],
                            activity['datacube'], version, activity['tileid'], date[0:10], band)
                        activity['sk'] = activity['date'] + activity['dataset']
                        activity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        # Check if we have already done and no need to do it again
                        response = services.get_activity_item({'id': activity['dynamoKey'], 'sk': activity['sk'] })
                        if 'Item' in response:
                            if not force and response['Item']['mystatus'] == 'DONE' \
                                    and services.s3_file_exists(key=activity['ARDfile']):
                                next_step(services, activity)
                                continue

                        # Re-schedule a merge-warped
                        activity['mystatus'] = 'NOTDONE'
                        activity['mystart'] = 'SSSS-SS-SS'
                        activity['myend'] = 'EEEE-EE-EE'
                        activity['efficacy'] = '0'
                        activity['cloudratio'] = '100'

                        # Send to queue to activate merge lambda
                        services.put_item_kinesis(activity)
                        services.send_to_sqs(activity)


def merge_warped(self, activity):
    logger.info('==> start MERGE')
    services = self.services
    key = activity['ARDfile']
    bucket_name = activity['bucket_name']
    prefix = services.get_s3_prefix(bucket_name)

    mystart = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['mystart'] = mystart
    activity['mystatus'] = 'DONE'
    satellite = activity.get('satellite')

    # Flag to force merge generation without cache
    force = activity.get('force')

    try:
        # If ARDfile already exists, update activitiesTable and chech if this merge is the last one for the mosaic
        if not force and services.s3_file_exists(bucket_name=bucket_name, key=key):
            efficacy = 0
            cloudratio = 100
            try:
                with rasterio.open('{}{}'.format(prefix, key)) as src:
                    values = src.read(1)
                    if activity['band'] == activity['quality_band']:
                        efficacy, cloudratio = qa_statistics(values)

                # Update entry in DynamoDB
                activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                activity['efficacy'] = '{}'.format(int(efficacy))
                activity['cloudratio'] = '{}'.format(int(cloudratio))
                services.put_item_kinesis(activity)

                key = '{}activities/{}{}.json'.format(activity['dirname'], activity['dynamoKey'], activity['date'])
                services.save_file_S3(bucket_name=bucket_name, key=key, activity=activity)
                return
            except:
                _ = services.delete_file_S3(bucket_name=bucket_name, key=key)

        # Lets warp and merge
        resx = float(activity['resx'])
        resy = float(activity['resy'])
        xmin = float(activity['xmin'])
        ymax = float(activity['ymax'])
        dist_x = float(activity['dist_x'])
        dist_y = float(activity['dist_y'])
        nodata = int(activity['nodata'])
        band = activity['band']

        num_pixel_x = round(dist_x / resx)
        num_pixel_y = round(dist_y / resy)
        new_res_x = dist_x / num_pixel_x
        new_res_y = dist_y / num_pixel_y

        numcol = num_pixel_x
        numlin = num_pixel_y

        transform = Affine(new_res_x, 0, xmin, 0, -new_res_y, ymax)

        is_sentinel_landsat_quality_fmask = ('LANDSAT' in satellite or satellite == 'SENTINEL-2') and band == activity['quality_band']
        source_nodata = 0

        # Quality band is resampled by nearest, other are bilinear
        if band == activity['quality_band']:
            resampling = Resampling.nearest

            nodata = 0

            if is_sentinel_landsat_quality_fmask:
                nodata = 255  # temporally set nodata to 255 in order to reproject without losing valid 0 values
                source_nodata = nodata

            raster = numpy.zeros((numlin, numcol,), dtype=numpy.uint16)
            raster_merge = numpy.full((numlin, numcol,), dtype=numpy.uint16, fill_value=source_nodata)
            raster_mask = numpy.ones((numlin, numcol,), dtype=numpy.uint16)
        else:
            resampling = Resampling.bilinear
            raster = numpy.zeros((numlin, numcol,), dtype=numpy.int16)
            raster_merge = numpy.full((numlin, numcol,), fill_value=nodata, dtype=numpy.int16)

        # For all files
        template = None

        for url in activity['links']:
            with rasterio.Env(CPL_CURL_VERBOSE=False):
                with rasterio.open(url) as src:
                    kwargs = src.meta.copy()
                    kwargs.update({
                        'crs': activity['srs'],
                        'transform': transform,
                        'width': numcol,
                        'height': numlin
                    })

                    if src.profile['nodata'] is not None:
                        source_nodata = src.profile['nodata']

                    elif 'LANDSAT' in satellite and band != activity['quality_band']:
                        source_nodata = nodata if src.profile['dtype'] == 'int16' else 0

                    elif 'CBERS' in satellite and band != activity['quality_band']:
                        source_nodata = nodata

                    kwargs.update({
                        'nodata': source_nodata
                    })

                    with MemoryFile() as memfile:
                        with memfile.open(**kwargs) as dst:
                            reproject(
                                source=rasterio.band(src, 1),
                                destination=raster,
                                src_transform=src.transform,
                                src_crs=src.crs,
                                dst_transform=transform,
                                dst_crs=activity['srs'],
                                src_nodata=source_nodata,
                                dst_nodata=nodata,
                                resampling=resampling)

                            if band != activity['quality_band'] or is_sentinel_landsat_quality_fmask:
                                valid_data_scene = raster[raster != nodata]
                                raster_merge[raster != nodata] = valid_data_scene.reshape(numpy.size(valid_data_scene))
                            else:
                                raster_merge = raster_merge + raster * raster_mask
                                raster_mask[raster != nodata] = 0

                            if template is None:
                                template = dst.profile
                                if band != activity['quality_band']:
                                    template['dtype'] = 'int16'
                                    template['nodata'] = nodata

        # Evaluate cloud cover and efficacy if band is quality
        efficacy = 0
        cloudratio = 100
        if activity['band'] == activity['quality_band']:
            raster_merge, efficacy, cloudratio = getMask(raster_merge, satellite)
            template.update({'dtype': 'uint8'})
            nodata = 255

            # Save merged image on S3
            create_cog_in_s3(services, template, key, raster_merge, True, nodata, bucket_name)
        else:
            create_cog_in_s3(services, template, key, raster_merge, False, None, bucket_name)

        # Update entry in DynamoDB
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        activity['efficacy'] = str(int(efficacy))
        activity['cloudratio'] = str(int(cloudratio))
        activity['new_resolution_x'] = str(new_res_x)
        activity['new_resolution_y'] = str(new_res_y)
        services.put_item_kinesis(activity)

        key = '{}activities/{}{}.json'.format(activity['dirname'], activity['dynamoKey'], activity['date'])
        services.save_file_S3(bucket_name=bucket_name, key=key, activity=activity)

    except Exception as e:
        # Update entry in DynamoDB
        activity['mystatus'] = 'ERROR'
        activity['errors'] = dict(
            step='merge',
            message=str(e)
        )
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        services.put_item_kinesis(activity)


###############################
# BLEND
###############################
def next_blend(services, mergeactivity):
    # Fill the blend activity from merge activity
    blendactivity = {}
    blendactivity['action'] = 'blend'
    blendactivity['datacube'] = mergeactivity['datacube_orig_name']
    for key in ['datasets', 'satellite', 'bands', 'quicklook', 'srs', 'functions',
                'tileid', 'start', 'end', 'dirname', 'nodata', 'bucket_name', 'quality_band',
                'internal_bands', 'indexes', 'force', 'version']:
        blendactivity[key] = mergeactivity.get(key, '')
    blendactivity['totalInstancesToBeDone'] = len(blendactivity['bands']) + len(blendactivity['internal_bands'])

    # Create  dynamoKey for the blendactivity record
    blendactivity['dynamoKey'] = encode_key(blendactivity, ['action','datacube','tileid','start','end'])

    # Fill the blendactivity fields with data for quality band from the DynamoDB merge records
    blendactivity['scenes'] = {}
    mergeactivity['band'] = mergeactivity['quality_band']
    blendactivity['band'] = mergeactivity['quality_band']
    _ = fill_blend(services, mergeactivity, blendactivity)

    # Reset mycount in  activitiesControlTable
    activitiesControlTableKey = blendactivity['dynamoKey']
    services.put_control_table(activitiesControlTableKey, 0)

    # If no quality file was found for this tile/period, register it in DynamoDB and go on
    if blendactivity['instancesToBeDone'] == 0:
        blendactivity['sk'] = 'ALLBANDS'
        blendactivity['mystatus'] = 'ERROR'
        blendactivity['errors'] = dict(
            step='next_blend',
            message='not quality file was found for this tile/period'
        )
        blendactivity['mystart'] = 'SSSS-SS-SS'
        blendactivity['myend'] = 'EEEE-EE-EE'
        blendactivity['efficacy'] = '0'
        blendactivity['cloudratio'] = '100'
        blendactivity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        services.put_item_kinesis(blendactivity)
        return False

    # Fill the blendactivity fields with data for the other bands from the DynamoDB merge records
    for band in (blendactivity['bands'] + blendactivity['internal_bands']):
        # if internal process, duplique first band and set the process in activity
        internal_band = band if band in blendactivity['internal_bands'] else False
        if internal_band:
            band = blendactivity['bands'][0]

        mergeactivity['band'] = band
        blendactivity['band'] = band
        _ = fill_blend(services, mergeactivity, blendactivity, internal_band)
        
        blendactivity['sk'] = internal_band if internal_band else band

        # Check if we are doing it again and if we have to do it because a different number of ARDfiles is present
        response = services.get_activity_item(
            {'id': blendactivity['dynamoKey'], 'sk': internal_band if internal_band else band })

        if 'Item' in response \
                and response['Item']['mystatus'] == 'DONE' \
                and response['Item']['instancesToBeDone'] == blendactivity['instancesToBeDone']:

            exists = True
            for func in blendactivity['functions']:
                if func == 'IDT' or (func == 'MED' and internal_band == 'PROVENANCE'): continue
                if not services.s3_file_exists(bucket_name=mergeactivity['bucket_name'], key=blendactivity['{}file'.format(func)]):
                    exists = False
            if not blendactivity.get('force') and exists:
                blendactivity['mystatus'] = 'DONE'
                next_step(services, blendactivity)
                continue

        # Blend has not been performed, do it
        blendactivity['mystatus'] = 'NOTDONE'
        blendactivity['mystart'] = 'SSSS-SS-SS'
        blendactivity['myend'] = 'EEEE-EE-EE'
        blendactivity['efficacy'] = '0'
        blendactivity['cloudratio'] = '100'
        blendactivity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Create an entry in dynamoDB for each band blend activity (quality band is not an entry in DynamoDB)
        key = '{}activities/{}.json'.format(blendactivity['dirname'], blendactivity['dynamoKey'])
        services.save_file_S3(bucket_name=blendactivity['bucket_name'], key=key, activity=blendactivity)
        services.put_item_kinesis(blendactivity)
        services.send_to_sqs(blendactivity)

        # Leave room for next band in blendactivity
        for datedataset in blendactivity['scenes']:
            if band in blendactivity['scenes'][datedataset]['ARDfiles'] \
                and band != mergeactivity['quality_band']:
                del blendactivity['scenes'][datedataset]['ARDfiles'][band]
    return True

def fill_blend(services, mergeactivity, blendactivity, internal_band=False):
    # Fill blend activity fields with data for band from the DynamoDB merge records
    band = blendactivity['band']
    blendactivity['instancesToBeDone'] = 0

    cube_version = blendactivity['version']

    # Query dynamoDB to get all merged
    items = []
    for date in mergeactivity['list_dates']:
        mergeactivity['date_formated'] = date

        # verify if internal process
        if internal_band:
            blendactivity['internal_band'] = internal_band
        dynamoKey = encode_key(mergeactivity, ['action','datacube','tileid','date_formated','band'])
        
        response = services.get_activities_by_key(dynamoKey)
        if 'Items' not in response or len(response['Items']) == 0:
            return False
        items += response['Items']

    for item in items:
        if item['mystatus'] != 'DONE':
            return False
        activity = json.loads(item['activity'])
        datedataset = item['sk']
        if datedataset not in blendactivity['scenes']:
            blendactivity['scenes'][datedataset] = {}
            blendactivity['scenes'][datedataset]['efficacy'] = item['efficacy']
            blendactivity['scenes'][datedataset]['date'] = activity['date']
            blendactivity['scenes'][datedataset]['dataset'] = activity['dataset']
            blendactivity['scenes'][datedataset]['satellite'] = activity['satellite']
            blendactivity['scenes'][datedataset]['cloudratio'] = item['cloudratio']
            blendactivity['scenes'][datedataset]['resolution'] = activity['resolution']
        if 'ARDfiles' not in blendactivity['scenes'][datedataset]:
            blendactivity['scenes'][datedataset]['ARDfiles'] = {}
        basename = os.path.basename(activity['ARDfile'])
        blendactivity['scenes'][datedataset]['ARDfiles'][band] = basename

    blendactivity['instancesToBeDone'] += len(items)
    if band != blendactivity['quality_band']:
        for function in blendactivity['functions']:
            if func == 'IDT': continue
            cube_id = '{}_{}'.format(blendactivity['datacube'], function)
            blendactivity['{}file'.format(function)] = '{0}/{5}/{1}/{2}_{3}/{0}_{5}_{1}_{2}_{3}_{4}.tif'.format(
                cube_id, blendactivity['tileid'], blendactivity['start'], blendactivity['end'], band, cube_version)
    else:
        # quality band generate only STK composite
        cube_id = '{}_{}'.format(blendactivity['datacube'], 'STK')
        blendactivity['{}file'.format('STK')] = '{0}/{5}/{1}/{2}_{3}/{0}_{5}_{1}_{2}_{3}_{4}.tif'.format(
            cube_id, blendactivity['tileid'], blendactivity['start'], blendactivity['end'], band, cube_version)
    return True

def blend(self, activity):
    logger.info('==> start BLEND')
    services = self.services

    activity['mystart'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['sk'] = activity['band'] if not activity.get('internal_band') else activity['internal_band']
    bucket_name = activity['bucket_name']
    prefix = services.get_s3_prefix(bucket_name)

    band = activity['band']
    numscenes = len(activity['scenes'])

    nodata = int(activity.get('nodata', -9999))
    if band == activity['quality_band']:
        nodata = 255

    # Check if band ARDfiles are in activity
    for datedataset in activity['scenes']:
        if band not in activity['scenes'][datedataset]['ARDfiles']:
            activity['mystatus'] = 'ERROR'
            activity['errors'] = dict(
                step='blend',
                message='ERROR band {}'.format(band)
            )
            services.put_item_kinesis(activity)
            return

    try:
        # Get basic information (profile) of input files
        keys = list(activity['scenes'].keys())
        filename = os.path.join(
            prefix + activity['dirname'],
            activity['scenes'][keys[0]]['date'],
            activity['scenes'][keys[0]]['ARDfiles'][band])

        with rasterio.open(filename) as src:
            profile = src.profile
            tilelist = list(src.block_windows())

        # Order scenes based in efficacy/resolution
        mask_tuples = []
        for key in activity['scenes']:
            scene = activity['scenes'][key]
            efficacy = int(scene['efficacy'])
            resolution = int(scene['resolution'])
            mask_tuples.append((100.*efficacy/resolution,key))

        # Open all input files and save the datasets in two lists, one for masks and other for the current band.
        # The list will be ordered by efficacy/resolution
        masklist = []
        bandlist = []
        for m in sorted(mask_tuples, reverse=True):
            key = m[1]
            efficacy = m[0]
            scene = activity['scenes'][key]

            # MASK -> Quality
            filename = os.path.join(
                prefix + activity['dirname'],
                scene['date'],
                scene['ARDfiles'][activity['quality_band']])
            try:
                masklist.append(rasterio.open(filename))
            except:
                activity['mystatus'] = 'ERROR'
                activity['errors'] = dict(
                    step= 'blend',
                    message='ERROR {}'.format(os.path.basename(filename))
                )
                services.put_item_kinesis(activity)
                return

            # BANDS
            filename = os.path.join(
                prefix + activity['dirname'],
                scene['date'],
                scene['ARDfiles'][band])
            try:
                bandlist.append(rasterio.open(filename))
            except:
                activity['mystatus'] = 'ERROR'
                activity['errors'] = dict(
                    step= 'blend',
                    message='ERROR {}'.format(os.path.basename(filename))
                )
                services.put_item_kinesis(activity)
                return

        # Build the raster to store the output images.
        width = profile['width']
        height = profile['height']

        # STACK and MED will be generated in memory
        stack_raster = numpy.full((height, width), dtype=profile['dtype'], fill_value=nodata)
        if 'MED' in activity['functions']:
            median_raster = numpy.full((height, width), dtype=profile['dtype'], fill_value=nodata)

        # Build the stack total observation
        build_total_observation = activity.get('internal_band') == 'TOTALOB'
        if build_total_observation:
            stack_total_observation = numpy.zeros((height, width), dtype=numpy.uint8)

        # create file to save clear observation, total oberservation and provenance
        build_clear_observation = activity.get('internal_band') == 'CLEAROB'
        if build_clear_observation:
            clear_ob_file = '/tmp/clearob.tif'
            clear_ob_profile = profile.copy()
            clear_ob_profile['dtype'] = 'uint8'
            clear_ob_profile['nodata'] = 255
            clear_ob_dataset = rasterio.open(clear_ob_file, mode='w', **clear_ob_profile)

        # Build the stack total observation
        build_provenance = activity.get('internal_band') == 'PROVENANCE'
        if build_provenance:
            provenance_array = numpy.full((height, width), dtype=numpy.int16, fill_value=-1)

        for _, window in tilelist:
            # Build the stack to store all images as a masked array. At this stage the array will contain the masked data
            stackMA = numpy.ma.zeros((numscenes, window.height, window.width), dtype=numpy.int16)

            notdonemask = numpy.ones(shape=(window.height, window.width), dtype=numpy.bool_)

            row_offset = window.row_off + window.height
            col_offset = window.col_off + window.width

            # For all pair (quality,band) scenes
            for order in range(numscenes):
                # Read both chunk of Merge and Quality, respectively.
                ssrc = bandlist[order]
                msrc = masklist[order]
                raster = ssrc.read(1, window=window)
                mask = msrc.read(1, window=window)

                if build_total_observation:
                    copy_mask = numpy.array(mask, copy=True)

                # Mask valid data (0 and 1) as True
                mask[mask < 2] = 1
                # Mask cloud/snow/shadow/no-data as False
                mask[mask >= 2] = 0
                # Ensure that Raster noda value (-9999 maybe) is set to False
                mask[raster == nodata] = 0

                # Create an inverse mask value in order to pass to numpy masked array
                # True => nodata
                bmask = numpy.invert(mask.astype(numpy.bool_))

                # Use the mask to mark the fill (0) and cloudy (2) pixels
                stackMA[order] = numpy.ma.masked_where(bmask, raster)

                if build_total_observation:
                    # Copy Masked values in order to stack total observation
                    copy_mask[copy_mask <= 4] = 1
                    copy_mask[copy_mask >= 5] = 0

                    stack_total_observation[window.row_off: row_offset, window.col_off: col_offset] += copy_mask.astype(numpy.uint8)

                # Get current observation file name
                if build_provenance:
                    file_name = Path(bandlist[order].name).stem
                    file_date = file_name.split('_')[4]
                    day_of_year = datetime.strptime(file_date, '%Y-%m-%d').timetuple().tm_yday

                # Find all no data in destination STACK image
                stack_raster_where_nodata = numpy.where(
                    stack_raster[window.row_off: row_offset, window.col_off: col_offset] == nodata
                )

                # Turns into a 1-dimension
                stack_raster_nodata_pos = numpy.ravel_multi_index(stack_raster_where_nodata,
                                                                    stack_raster[window.row_off: row_offset,
                                                                    window.col_off: col_offset].shape)

                # Find all valid/cloud in destination STACK image
                raster_where_data = numpy.where(raster != nodata)
                raster_data_pos = numpy.ravel_multi_index(raster_where_data, raster.shape)

                # Match stack nodata values with observation
                # stack_raster_where_nodata && raster_where_data
                intersect_ravel = numpy.intersect1d(stack_raster_nodata_pos, raster_data_pos)

                if len(intersect_ravel):
                    where_intersec = numpy.unravel_index(intersect_ravel, raster.shape)
                    stack_raster[window.row_off: row_offset, window.col_off: col_offset][where_intersec] = raster[where_intersec]

                    if build_provenance:
                        provenance_array[window.row_off: row_offset, window.col_off: col_offset][where_intersec] = day_of_year

                # Identify what is needed to stack, based in Array 2d bool
                todomask = notdonemask * numpy.invert(bmask)

                # Find all positions where valid data matches.
                clear_not_done_pixels = numpy.where(numpy.logical_and(todomask, mask.astype(numpy.bool)))

                # Override the STACK Raster with valid data.
                stack_raster[window.row_off: row_offset, window.col_off: col_offset][clear_not_done_pixels] = raster[clear_not_done_pixels]

                if build_provenance:
                    # Mark day of year to the valid pixels
                    provenance_array[window.row_off: row_offset, window.col_off: col_offset][clear_not_done_pixels] = day_of_year

                # Update what was done.
                notdonemask = notdonemask * bmask

            if 'MED' in activity['functions']:
                median = numpy.ma.median(stackMA, axis=0).data
                median[notdonemask.astype(numpy.bool_)] = nodata
                median_raster[window.row_off: row_offset, window.col_off: col_offset] = median.astype(profile['dtype'])

            if build_clear_observation:
                count_raster = numpy.ma.count(stackMA, axis=0)
                clear_ob_dataset.nodata = 255
                clear_ob_dataset.write(count_raster.astype(clear_ob_profile['dtype']), window=window, indexes=1)

        # Close all input dataset
        for order in range(numscenes):
            bandlist[order].close()
            masklist[order].close()

        # Evaluate cloudcover
        cloudcover = 100. * ((height * width - numpy.count_nonzero(stack_raster)) / (height * width))
        activity['cloudratio'] = int(cloudcover)

        # Upload the CLEAROB dataset
        if build_clear_observation:
            clear_ob_dataset.close()
            clear_ob_dataset = None
            for func in activity['functions']:
                if func == 'IDT': continue
                key_clearob = '_'.join(activity['{}file'.format(func)].split('_')[:-1]) + '_CLEAROB.tif'
                services.upload_file_S3(clear_ob_file, key_clearob, {'ACL': 'public-read'}, bucket_name=bucket_name)
            os.remove(clear_ob_file)

        # Upload the PROVENANCE dataset
        if build_provenance and 'STK' in activity['functions']:
            provenance_profile = profile.copy()
            provenance_profile.pop('nodata',  -1)
            provenance_profile['dtype'] = 'int16'
            provenance_key = '_'.join(activity['STKfile'].split('_')[:-1]) + '_PROVENANCE.tif'
            create_cog_in_s3(
                services, provenance_profile, provenance_key, provenance_array, 
                False, None, bucket_name)

        # Upload the TOTALOB dataset
        if build_total_observation:
            total_observation_profile = profile.copy()
            total_observation_profile.pop('nodata', None)
            total_observation_profile['dtype'] = 'uint8'
            for func in activity['functions']:
                if func == 'IDT': continue
                total_ob_key = '_'.join(activity['{}file'.format(func)].split('_')[:-1]) + '_TOTALOB.tif'
                create_cog_in_s3(
                    services, total_observation_profile, total_ob_key, stack_total_observation, 
                    False, None, bucket_name)

        # Create and upload the STACK dataset
        if 'STK' in activity['functions']:
            if not activity.get('internal_band'):
                create_cog_in_s3(
                    services, profile, activity['STKfile'], stack_raster, 
                    (band != activity['quality_band']), nodata, bucket_name)

        # Create and upload the STACK dataset
        if 'MED' in activity['functions']:
            if not activity.get('internal_band'):
                create_cog_in_s3(
                    services, profile, activity['MEDfile'], median_raster, 
                    (band != activity['quality_band']), nodata, bucket_name)

        # Update status and end time in DynamoDB
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        activity['mystatus'] = 'DONE'
        services.put_item_kinesis(activity)

    except Exception as e:
        # Update entry in DynamoDB
        activity['mystatus'] = 'ERROR'
        activity['errors'] = dict(
            step='blend',
            message=str(e)
        )
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        services.put_item_kinesis(activity)


###############################
# POS BLEND
###############################
def next_posblend(services, blendactivity):
    # Fill the blend activity from merge activity
    blend_dynamo_key = blendactivity['dynamoKey']
    posblendactivity = blendactivity
    posblendactivity['action'] = 'posblend'
    posblendactivity['totalInstancesToBeDone'] = len(posblendactivity['indexes']) * (len(posblendactivity['scenes'].keys()) + 1) # Irregular and regular

    # Reset mycount in activitiesControlTable
    if posblendactivity['action'] not in posblendactivity['dynamoKey']:
        posblendactivity['dynamoKey'] = blend_dynamo_key.replace('blend', posblendactivity['action'])
    activitiesControlTableKey = posblendactivity['dynamoKey']
    services.put_control_table(activitiesControlTableKey, 0)

    posblendactivity['indexesToBe'] = {}
    for index in posblendactivity['indexes']:
        i_name = index['name']
        posblendactivity['indexesToBe'][i_name] = {}

        for band in index['bands']:
            # get Blend activity
            response = services.get_activity_item({'id': blend_dynamo_key, 'sk': band['name']})
            item = response['Item']
            activity = json.loads(item['activity'])

            for func in posblendactivity['functions']:
                posblendactivity['indexesToBe'][i_name][func] = posblendactivity['indexesToBe'][i_name].get(func, {})

                if func == 'IDT':
                    dates = activity['scenes'].keys()
                    for date_with_dataset in dates:
                        scene = activity['scenes'][date_with_dataset]

                        date = scene['date']
                        posblendactivity['indexesToBe'][i_name][func][date] = posblendactivity['indexesToBe'][i_name][func].get(date, {})
                        path_band = '{}{}/{}'.format(activity['dirname'], date, scene['ARDfiles'][band['name']])
                        posblendactivity['indexesToBe'][i_name][func][date][band['common_name']] = path_band
                else:
                    posblendactivity['indexesToBe'][i_name][func][band['common_name']] = activity['{}file'.format(func)]

    for index in posblendactivity['indexes']:
        # create and dispatch one activity to irregular cube and one to regular cubes (each index)
        for i in ['', 'IDT']:
            posblendactivity['sk'] = '{}{}'.format(index['name'], i)
            # Blend has not been performed, do it
            posblendactivity['mystatus'] = 'NOTDONE'
            posblendactivity['mystart'] = 'SSSS-SS-SS'
            posblendactivity['myend'] = 'EEEE-EE-EE'
            posblendactivity['efficacy'] = '0'
            posblendactivity['cloudratio'] = '100'
            posblendactivity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if i == 'IDT':
                dates = activity['scenes'].keys()
                for date_with_dataset in dates:
                    scene = activity['scenes'][date_with_dataset]
                    date = scene['date']
                    posblendactivity['sk'] = '{}{}{}'.format(index['name'], i, date)
            
                    # Create an entry in dynamoDB for each band blend activity (quality band is not an entry in DynamoDB)
                    key = '{}activities/{}.json'.format(posblendactivity['dirname'], posblendactivity['dynamoKey'])
                    services.save_file_S3(bucket_name=posblendactivity['bucket_name'], key=key, activity=posblendactivity)
                    services.put_item_kinesis(posblendactivity)
                    services.send_to_sqs(posblendactivity)
            else:
                # Create an entry in dynamoDB for each band blend activity (quality band is not an entry in DynamoDB)
                key = '{}activities/{}.json'.format(posblendactivity['dirname'], posblendactivity['dynamoKey'])
                services.save_file_S3(bucket_name=posblendactivity['bucket_name'], key=key, activity=posblendactivity)
                services.put_item_kinesis(posblendactivity)
                services.send_to_sqs(posblendactivity)

    return True

def posblend(self, activity):
    logger.info('==> start POS BLEND')
    services = self.services

    bucket_name = activity['bucket_name']
    activity['mystart'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        sk = activity['sk']
        is_identity = 'IDT' in activity['sk']
        
        if is_identity:
            sk = sk.replace('IDT', '')
            index = activity['indexesToBe'][sk[:-10]]
            for date in index['IDT'].keys():
                if date in sk:
                    bands = index['IDT'][date]
                    create_index(services, sk.replace(date, ''), bands, bucket_name)
        else:
            index = activity['indexesToBe'][sk]
            for func in activity['functions']:
                if func == 'IDT': continue
                bands = index[func]
                create_index(services, sk, bands, bucket_name)
                
        # Update status and end time in DynamoDB
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        activity['mystatus'] = 'DONE'
        services.put_item_kinesis(activity)
    
    except Exception as e:
        # Update entry in DynamoDB
        logger.error(e, exc_info=True)
        logger.error('erro', exc_info=True)
        activity['mystatus'] = 'ERROR'
        activity['errors'] = dict(
            step='posblend',
            message=str(e)
        )
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        services.put_item_kinesis(activity)


###############################
# PUBLISH
###############################
def next_publish(services, posblendactivity):
    # Fill the publish activity from blend activity
    publishactivity = {}
    for key in ['datacube','satellite','datasets','bands','quicklook','tileid','start','end', \
        'dirname', 'cloudratio', 'bucket_name', 'quality_band', 'internal_bands', \
        'functions', 'indexes', 'version']:
        publishactivity[key] = posblendactivity.get(key)
    publishactivity['action'] = 'publish'

    # Create  dynamoKey for the publish activity
    publishactivity['dynamoKey'] = encode_key(publishactivity, ['action','datacube','tileid','start','end'])

    # Get information from blended bands
    response = services.get_activities_by_key(posblendactivity['dynamoKey'].replace('posblend', 'blend'))

    items = response['Items']
    publishactivity['scenes'] = {}
    publishactivity['blended'] = {}
    example_file_name = ''
    for item in items:
        activity = json.loads(item['activity'])
        band = item['sk']
        if band not in publishactivity['bands']: continue

        # Get ARD files
        for datedataset in activity['scenes']:
            scene = activity['scenes'][datedataset]
            if datedataset not in publishactivity['scenes']:
                publishactivity['scenes'][datedataset] = {'ARDfiles' : {}}
                publishactivity['scenes'][datedataset]['ARDfiles'][publishactivity['quality_band']] = scene['ARDfiles'][publishactivity['quality_band']]
                publishactivity['scenes'][datedataset]['date'] = scene['date']
                publishactivity['scenes'][datedataset]['dataset'] = scene['dataset']
                publishactivity['scenes'][datedataset]['satellite'] = scene['satellite']
                publishactivity['scenes'][datedataset]['cloudratio'] = scene['cloudratio']
                # add indexes to publish
                for index in publishactivity['indexes']:
                    # ex: LC8_30_090096_2019-01-28_fMask.tif => LC8_30_090096_2019-01-28_NDVI.tif
                    quality_file = scene['ARDfiles'][publishactivity['quality_band']]
                    index_file_name = '_'.join(quality_file.split('_')[:-1]) + '_{}.tif'.format(index['name'])
                    publishactivity['scenes'][datedataset]['ARDfiles'][index['name']] = index_file_name
            publishactivity['scenes'][datedataset]['ARDfiles'][band] = scene['ARDfiles'][band]

        # Get blended files
        publishactivity['blended'][band] = {}
        for func in publishactivity['functions']:
            if func == 'IDT': continue
            if func == 'MED' and band == publishactivity['quality_band']: continue
            key_file = '{}file'.format(func)
            publishactivity['blended'][band][key_file] = activity[key_file]
            example_file_name = activity[key_file]

    # Create indices to catalog CLEAROB, TOTALOB, PROVENANCE, ...
    for internal_band in publishactivity['internal_bands']:
        publishactivity['blended'][internal_band] = {}
        for func in publishactivity['functions']:
            if func == 'IDT': continue
            if func == 'MED' and internal_band == 'PROVENANCE': continue
            key_file = '{}file'.format(func)
            file_name = '_'.join(example_file_name.split('_')[:-1]) + '_{}.tif'.format(internal_band)
            publishactivity['blended'][internal_band][key_file] = file_name

    # Create indices to catalog NIR, NDVI ...
    for index in publishactivity['indexes']:
        publishactivity['blended'][index['name']] = {}
        for func in publishactivity['functions']:
            if func == 'IDT': continue
            key_file = '{}file'.format(func)
            file_name = '_'.join(example_file_name.split('_')[:-1]) + '_{}.tif'.format(index['name'])
            publishactivity['blended'][index['name']][key_file] = file_name

    publishactivity['sk'] = 'ALLBANDS'
    publishactivity['mystatus'] = 'NOTDONE'
    publishactivity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    publishactivity['mystart'] = 'SSSS-SS-SS'
    publishactivity['myend'] = 'EEEE-EE-EE'
    publishactivity['efficacy'] = '0'
    publishactivity['cloudratio'] = '100'
    publishactivity['instancesToBeDone'] = len(publishactivity['bands']) - 1
    publishactivity['totalInstancesToBeDone'] = 1

    # Launch activity
    services.put_item_kinesis(publishactivity)
    services.send_to_sqs(publishactivity)

def publish(self, activity):
    logger.info('==> start PUBLISH')
    services = self.services
    bucket_name = activity['bucket_name']
    prefix = services.get_s3_prefix(bucket_name)

    activity['mystart'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        identity_cube = '_'.join(activity['datacube'].split('_')[0:2])

        # GENERATE QUICKLOOK's and REGISTER ITEMS in DB
        ## for CUBES (MEDIAN, STACK ...)
        qlbands = activity['quicklook']
        for function in activity['functions']:
            if function == 'IDT': continue

            cube_name = get_cube_name(activity['datacube'], function)
            cube = Collection.query().filter(
                Collection.name == cube_name,
                Collection.version == int(activity['version'][-3:])
            ).first()
            if not cube:
                raise Exception(f'cube {cube_name} - {activity["version"]} not found!')

            general_scene_id = '{}_{}_{}_{}_{}'.format(
                cube_name, activity['version'], activity['tileid'], activity['start'], activity['end'])

            # Generate quicklook
            qlfiles = []
            for band in qlbands:
                qlfiles.append(prefix + activity['blended'][band][function + 'file'])
    
            png_name = generateQLook(general_scene_id, qlfiles)
            png_file_name = Path(png_name).name
            if png_name is None:
                raise Exception(f'publish - Error generateQLook for {general_scene_id}')

            dirname_ql = activity['dirname'].replace(f'{identity_cube}/', f'{cube_name}/')
            range_date = f'{activity["start"]}_{activity["end"]}'
            s3_pngname = os.path.join(dirname_ql, range_date, png_file_name)
            services.upload_file_S3(png_name, s3_pngname, {'ACL': 'public-read'}, bucket_name=bucket_name)
            os.remove(png_name)

            quicklook_url = f'{bucket_name}/{s3_pngname}'

            # register items in DB
            with db.session.begin_nested():
                item = Item.query().filter(
                    Item.name == general_scene_id,
                    Item.collection_id == cube.id).first()
                if not item:
                    tile = Tile.query().filter(
                        Tile.name == activity['tileid'],
                        Tile.grid_ref_sys_id == cube.grid_ref_sys_id
                    ).first()
                    
                    item = Item(
                        name=general_scene_id,
                        collection_id=cube.id,
                        tile_id=tile.id,
                        start_date=activity['start'],
                        end_date=activity['end'],
                        cloud_cover=float(activity['cloudratio'])
                    )


                assets = dict(
                    thumbnail=create_asset_definition(
                        services, bucket_name, str(s3_pngname), 'image/png', ['thumbnail'], quicklook_url)
                )

                # add 'assets'
                bands_by_cube = Band.query().filter(
                    Band.collection_id == cube.id
                ).all()

                indexes_list = [index['name'] for index in activity['indexes']]
                for band in (activity['bands'] + activity['internal_bands'] + indexes_list):
                    if not activity['blended'][band].get('{}file'.format(function)):
                        continue

                    band_model = next(filter(lambda b: str(b.name) == band, bands_by_cube), None)
                    if not band_model:
                        raise Exception(f'band {band} not found!')

                    relative_path = activity["blended"][band][function + "file"]        
                    full_path = f'{bucket_name}/{relative_path}'
                    assets[band_model.name] = create_asset_definition(
                        services, bucket_name,
                        relative_path, COG_MIME_TYPE, ['data'],
                        full_path, is_raster=True
                    )

                item.assets = assets
                item.updated = datetime.now()
                db.session.add(item)
            db.session.commit()


        ## for all ARD scenes (IDENTITY)
        for datedataset in activity['scenes']:
            scene = activity['scenes'][datedataset]

            cube_name = get_cube_name(activity['datacube'])
            cube = Collection.query().filter(
                Collection.name == cube_name,
                Collection.version == int(activity['version'][-3:])
            ).first()
            if not cube:
                raise Exception(f'cube {cube_name} - {activity["version"]} not found!')

            general_scene_id = '{}_{}_{}_{}'.format(
                cube_name, activity['version'], activity['tileid'], str(scene['date'])[0:10])

            # Generate quicklook
            qlfiles = []
            for band in qlbands:
                filename = os.path.join(prefix + activity['dirname'], str(scene['date'])[0:10], scene['ARDfiles'][band])
                qlfiles.append(filename)

            png_name = generateQLook(general_scene_id, qlfiles)
            png_file_name = Path(png_name).name
            if png_name is None:
                raise Exception(f'publish - Error generateQLook for {general_scene_id}')

            date = str(scene['date'])[0:10]
            s3_pngname = os.path.join(activity['dirname'], date, png_file_name)
            services.upload_file_S3(png_name, s3_pngname, {'ACL': 'public-read'}, bucket_name=bucket_name)
            os.remove(png_name)

            quicklook_url = f'{bucket_name}/{s3_pngname}'

            # register items in DB
            with db.session.begin_nested():
                item = Item.query().filter(
                    Item.name == general_scene_id,
                    Item.collection_id == cube.id).first()
                if not item:
                    tile = Tile.query().filter(
                        Tile.name == activity['tileid'],
                        Tile.grid_ref_sys_id == cube.grid_ref_sys_id
                    ).first()
                    
                    item = Item(
                        name=general_scene_id,
                        collection_id=cube.id,
                        tile_id=tile.id,
                        start_date=scene['date'],
                        end_date=scene['date'],
                        cloud_cover=float(scene['cloudratio'])
                    )

                assets = dict(
                    thumbnail=create_asset_definition(
                        services, bucket_name, str(s3_pngname), 'image/png', ['thumbnail'], quicklook_url)
                )

                # insert 'assets'
                bands_by_cube = Band.query().filter(
                    Band.collection_id == cube.id
                ).all()
                indexes_list = [index['name'] for index in activity['indexes']]
                for band in (activity['bands'] + indexes_list):
                    if band not in scene['ARDfiles']:
                        raise Exception(f'publish - problem - band {band} not in scene[files]')

                    band_model = next(filter(lambda b: str(b.name) == band, bands_by_cube), None)
                    if not band_model:
                        raise Exception(f'band {band} not found!')
                    
                    relative_path = os.path.join(activity['dirname'], str(scene['date'])[0:10], scene['ARDfiles'][band])
                    full_path = f'{bucket_name}/{relative_path}'
                    assets[band_model.name] = create_asset_definition(
                        services, bucket_name,
                        relative_path, COG_MIME_TYPE, ['data'],
                        full_path, is_raster=True
                    )
                
                item.assets = assets
                item.updated = datetime.now()
                db.session.add(item)
            db.session.commit()

        # Update status and end time in DynamoDB
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        activity['mystatus'] = 'DONE'
        services.put_item_kinesis(activity)

    except Exception as e:
        activity['mystatus'] = 'ERROR'
        activity['errors'] = dict(
            step='publish',
            message=str(e)
        )
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        services.put_item_kinesis(activity)
