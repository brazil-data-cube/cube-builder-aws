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
from geoalchemy2 import func
from rasterio.transform import Affine 
from rasterio.warp import reproject, Resampling, transform
from rasterio.merge import merge 
from rasterio.io import MemoryFile

from bdc_db.models.base_sql import BaseModel, db
from bdc_db.models import CollectionTile, CollectionItem, Tile, \
    Collection, Asset, Band

from .logger import logger
from .utils.builder import decode_periods, encode_key, \
    getMaskStats, getMask, generateQLook, get_cube_id, get_resolution_by_satellite


def orchestrate(datacube, cube_infos, tiles, start_date, end_date):
    # create collection_tiles
    tiles_by_grs = db.session() \
        .query(Tile, func.ST_AsText(func.ST_BoundingDiagonal(Tile.geom_wgs84))) \
        .filter(
            Tile.grs_schema_id == cube_infos.grs_schema_id,
            Tile.id.in_(tiles)
        ).all()

    collection_tiles = []
    tiles = list(set(tiles))
    tiles_infos = {}
    for tile in tiles:
        # verify tile exists
        tile_info = list(filter(lambda t: t[0].id == tile, tiles_by_grs))
        if not tile_info:
            return 'tile ({}) not found in GRS ({})'.format(tile, cube_infos.grs_schema_id), 404

        tiles_infos[tile] = tile_info[0]
        for function in ['IDENTITY', 'STK', 'MED']:
            cube_id = get_cube_id(datacube, function)
            collection_tile = CollectionTile.query().filter(
                CollectionTile.collection_id == cube_id,
                CollectionTile.grs_schema_id == cube_infos.grs_schema_id,
                CollectionTile.tile_id == tile
            ).first()
            if not collection_tile:
                collection_tiles.append(CollectionTile(
                    collection_id=cube_id,
                    grs_schema_id=cube_infos.grs_schema_id,
                    tile_id=tile
                ))
    BaseModel.save_all(collection_tiles)

    # get cube start_date if exists
    collections_items = CollectionItem.query().filter(
        CollectionItem.collection_id == cube_infos.id,
        CollectionItem.grs_schema_id == cube_infos.grs_schema_id
    ).order_by(CollectionItem.composite_start).all()
    cube_start_date = start_date
    if list(filter(lambda c_i: c_i.tile_id == tile, collections_items)):
        cube_start_date = collections_items[0].composite_start

    # get/mount timeline
    temporal_schema = cube_infos.temporal_composition_schema.temporal_schema
    step = cube_infos.temporal_composition_schema.temporal_composite_t
    timeline = decode_periods(temporal_schema.upper(), cube_start_date, end_date, int(step))

    # create collection items (old model => mosaic)
    items_id = []
    items = {}
    for datekey in sorted(timeline):
        requestedperiod = timeline[datekey]
        for periodkey in requestedperiod:
            (p_basedate, p_startdate, p_enddate) = periodkey.split('_')

            if start_date is not None and p_startdate < start_date : continue
            if end_date is not None and p_enddate > end_date : continue

            for tile in tiles:
                bbox = tiles_infos[tile][1].replace('LINESTRING(', '') \
                    .replace('LINESTRING Z (', '') \
                    .replace(')', '') \
                    .replace(' ', ',') \
                    .split(',')
                if len(bbox) != 4:
                    del bbox[2]
                    del bbox[-1]

                items[tile] = items.get(tile, {})
                items[tile]['bbox'] = ','.join(bbox)
                items[tile]['xmin'] = tiles_infos[tile][0].min_x
                items[tile]['ymax'] = tiles_infos[tile][0].max_y
                items[tile]['periods'] = items[tile].get('periods', {})

                item_id = '{}_{}_{}'.format(cube_infos.id, tile, p_basedate)
                if item_id not in items_id:
                    items_id.append(item_id)
                    if not list(filter(lambda c_i: c_i.id == item_id, collections_items)):
                        items[tile]['periods'][periodkey] = {
                            'collection': cube_infos.id,
                            'grs_schema_id': cube_infos.grs_schema_id,
                            'tile_id': tile,
                            'item_date': p_basedate,
                            'id': item_id,
                            'composite_start': p_startdate,
                            'composite_end': p_enddate,
                            'dirname': '{}/{}/'.format(get_cube_id(datacube), tile)
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
        if mycount == activity['totalInstancesToBeDone']:
            if activity['action'] == 'merge':
                next_blend(services, activity)
            elif activity['action'] == 'blend':
                next_publish(services, activity)


###############################
# MERGE
###############################
def prepare_merge(self, datacube, datasets, satellite, bands, quicklook, resx,
                  resy, nodata, numcol, numlin, block_size, crs, 
                  quality_band, force=False):
    services = self.services

    # Build the basics of the merge activity
    activity = {}
    activity['action'] = 'merge'
    activity['datacube_orig_name'] = datacube
    activity['datacube'] = get_cube_id(datacube)
    activity['datasets'] = datasets
    activity['satellite'] = satellite
    activity['bands'] = bands
    activity['quicklook'] = quicklook
    activity['resx'] = resx
    activity['resy'] = resy
    activity['numcol'] = numcol
    activity['numlin'] = numlin
    activity['nodata'] = nodata
    activity['block_size'] = block_size
    activity['srs'] = crs
    activity['bucket_name'] = services.bucket_name
    activity['url_stac'] = services.url_stac
    activity['quality_band'] = quality_band

    logger.info('prepare merge - Score {} items'.format(self.score['items']))

    # For all tiles
    for tileid in self.score['items']:
        activity['tileid'] = tileid
        # GET bounding box - tile ID
        activity['bbox'] = self.score['items'][tileid]['bbox']
        activity['xmin'] = self.score['items'][tileid]['xmin']
        activity['ymax'] = self.score['items'][tileid]['ymax']

        # For all periods
        for periodkey in self.score['items'][tileid]['periods']:
            activity['start'] = self.score['items'][tileid]['periods'][periodkey]['composite_start']
            activity['end'] = self.score['items'][tileid]['periods'][periodkey]['composite_end']
            activity['dirname'] = self.score['items'][tileid]['periods'][periodkey]['dirname']

            # When force is True, we must rebuild the merge
            if force:
                merge_control_key = encode_key(activity, ['action', 'datacube', 'tileid', 'start', 'end'])
                blend_control_key = 'blend{}_1M{}'.format(activity['datacube'],
                                                          encode_key(activity, ['tileid', 'start', 'end']))
                self.services.remove_control_by_key(merge_control_key)
                self.services.remove_control_by_key(blend_control_key)

            # Search all images
            self.score['items'][tileid]['periods'][periodkey]['scenes'] = services.search_STAC(activity)

            # Evaluate the number of dates, the number of scenes for each date and
            # the total amount merges that will be done
            number_of_datasets_dates = 0
            band = activity['bands'][0]
            list_dates = []
            for dataset in self.score['items'][tileid]['periods'][periodkey]['scenes'][band].keys():
                for date in self.score['items'][tileid]['periods'][periodkey]['scenes'][band][dataset].keys():
                    list_dates.append(str(date)[:10])
                    number_of_datasets_dates += 1
            activity['instancesToBeDone'] = number_of_datasets_dates
            activity['totalInstancesToBeDone'] = number_of_datasets_dates*len(activity['bands'])

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
            for band in self.score['items'][tileid]['periods'][periodkey]['scenes']:
                activity['band'] = band

                # For all datasets
                for dataset in self.score['items'][tileid]['periods'][periodkey]['scenes'][band]:
                    activity['dataset'] = dataset

                    # get resolution by satellite
                    activity['resolution'] = get_resolution_by_satellite(activity['satellite'])

                    # For all dates
                    for date in self.score['items'][tileid]['periods'][periodkey]['scenes'][band][dataset]:
                        activity['date'] = date[0:10]
                        activity['links'] = []

                        # Create the dynamoKey for the activity in DynamoDB
                        activity['dynamoKey'] = encode_key(activity, ['action','datacube','tileid','date','band'])

                        # Get all scenes that were acquired in the same date
                        for scene in self.score['items'][tileid]['periods'][periodkey]['scenes'][band][dataset][date]:
                            activity['links'].append(scene['link'])

                        # Continue filling the activity
                        activity['ARDfile'] = activity['dirname']+'{}/{}_{}_{}_{}.tif'.format(date[0:10],
                            activity['datacube'], activity['tileid'], date[0:10], band)
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
                        activity['force'] = force

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
    force = activity.pop('force', False)

    try:
        # If ARDfile already exists, update activitiesTable and chech if this merge is the last one for the mosaic
        if not force and services.s3_file_exists(bucket_name=bucket_name, key=key):
            efficacy = 0
            cloudratio = 100
            try:
                with rasterio.open('{}{}'.format(prefix, key)) as src:
                    values = src.read(1)
                    if activity['band'] == activity['quality_band']:
                        cloudratio, efficacy = getMaskStats(values)

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
        resx = int(activity['resx'])
        resy = int(activity['resy'])
        xmin = float(activity['xmin'])
        ymax = float(activity['ymax'])
        numcol = int(activity['numcol'])
        numlin = int(activity['numlin'])
        block_size = int(activity['block_size'])
        nodata = int(activity['nodata']) if 'nodata' in activity else -9999
        transform = Affine(resx, 0, xmin, 0, -resy, ymax)

        # Quality band is resampled by nearest, other are bilinear
        band = activity['band']
        if band == activity['quality_band']:
            resampling = Resampling.nearest

            raster = numpy.zeros((numlin, numcol,), dtype=numpy.uint16)
            raster_merge = numpy.zeros((numlin, numcol,), dtype=numpy.uint16)
            raster_mask = numpy.ones((numlin, numcol,), dtype=numpy.uint16)
            nodata = 0
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

                    source_nodata = 0

                    if src.profile['nodata'] is not None:
                        source_nodata = src.profile['nodata']
                    elif satellite == 'LANDSAT':
                        if band != activity['quality_band']:
                            source_nodata = nodata
                        else:
                            source_nodata = 1
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

                            if band != activity['quality_band']:
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
            template.update({'dtype': 'uint16'})

        # Save merged image on S3
        with MemoryFile() as memfile:
            template.update({
                'compress': 'LZW',
                'tiled': True,
                'interleave': 'pixel',
                'blockxsize': block_size,
                'blockysize': block_size
            })
            with memfile.open(**template) as riodataset:
                riodataset.nodata = nodata
                riodataset.write_band(1, raster_merge)
                riodataset.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
                riodataset.update_tags(ns='rio_overview', resampling='nearest')
            services.upload_fileobj_S3(memfile, key, {'ACL': 'public-read'}, bucket_name=bucket_name)

        # Update entry in DynamoDB
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        activity['efficacy'] = '{}'.format(int(efficacy))
        activity['cloudratio'] = '{}'.format(int(cloudratio))
        activity['raster_size_x'] = '{}'.format(numcol)
        activity['raster_size_y'] = '{}'.format(numlin)
        activity['block_size'] = '{}'.format(block_size)
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
    for key in ['datasets','satellite', 'bands','quicklook','xmin','ymax','srs',
                'tileid','start','end','dirname','nodata','bucket_name', 'quality_band']:
        blendactivity[key] = mergeactivity[key]
    blendactivity['totalInstancesToBeDone'] = len(blendactivity['bands'])-1

    # Create  dynamoKey for the blendactivity record
    blendactivity['dynamoKey'] = encode_key(blendactivity, ['action','datacube','tileid','start','end'])

    # Fill the blendactivity fields with data for quality band from the DynamoDB merge records
    blendactivity['scenes'] = {}
    blendactivity['band'] = mergeactivity['quality_band']
    mergeactivity['band'] = mergeactivity['quality_band']
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

    # Fill the blendactivity fields with data for the other bands from the DynamoDB merge records (quality band is not a blend entry in DynamoDB)
    for band in blendactivity['bands']:
        if band == blendactivity['quality_band'] or not blendactivity['datacube'].endswith('STK'):
            continue

        mergeactivity['band'] = band
        blendactivity['band'] = band
        blendactivity['sk'] = band
        _ = fill_blend(services, mergeactivity, blendactivity)

        # Check if we are doing it again and if we have to do it because a different number of ARDfiles is present
        response = services.get_activity_item({'id': blendactivity['dynamoKey'], 'sk': band })

        if 'Item' in response \
                and response['Item']['mystatus'] == 'DONE' \
                and response['Item']['instancesToBeDone'] == blendactivity['instancesToBeDone'] \
                and services.s3_file_exists(bucket_name=mergeactivity['bucket_name'], key=blendactivity['MEDfile']) \
                and services.s3_file_exists(bucket_name=mergeactivity['bucket_name'], key=blendactivity['STKfile']):
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
            if band in blendactivity['scenes'][datedataset]['ARDfiles']:
                del blendactivity['scenes'][datedataset]['ARDfiles'][band]
    return True

def fill_blend(services, mergeactivity, blendactivity):
    # Fill blend activity fields with data for band from the DynamoDB merge records
    band = blendactivity['band']
    blendactivity['instancesToBeDone'] = 0

    # Query dynamoDB to get all merged
    items = []
    for date in mergeactivity['list_dates']:
        mergeactivity['date_formated'] = date
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
            blendactivity['scenes'][datedataset]['raster_size_x'] = activity.get('raster_size_x')
            blendactivity['scenes'][datedataset]['raster_size_y'] = activity.get('raster_size_y')
            blendactivity['scenes'][datedataset]['block_size'] = activity.get('block_size')
            blendactivity['scenes'][datedataset]['resolution'] = activity['resolution']
        if 'ARDfiles' not in blendactivity['scenes'][datedataset]:
            blendactivity['scenes'][datedataset]['ARDfiles'] = {}
        basename = os.path.basename(activity['ARDfile'])
        blendactivity['scenes'][datedataset]['ARDfiles'][band] = basename

    blendactivity['instancesToBeDone'] += len(items)
    if band != blendactivity['quality_band']:
        for function in ['MED', 'STK']:
            cube_id = '{}_{}'.format(blendactivity['datacube'], function)
            blendactivity['{}file'.format(function)] = '{0}/{1}/{2}_{3}/{0}_{1}_{2}_{3}_{4}.tif'.format(
                cube_id, blendactivity['tileid'], blendactivity['start'], blendactivity['end'], band)
    return True

def blend(self, activity):
    logger.info('==> start BLEND')
    services = self.services

    activity['mystart'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['sk'] = activity['band']
    band = activity['band']
    numscenes = len(activity['scenes'])
    nodata = activity.get('nodata', -9999)
    bucket_name = activity['bucket_name']
    prefix = services.get_s3_prefix(bucket_name)

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
            profile.update({
                'compress': 'LZW',
                'tiled': True,
                'blockxsize': activity.get('chunk_size_x', 512),
                'blockysize': activity.get('chunk_size_y', 512)
            })
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
                activity['mystatus'] = 'ERROR',
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
                activity['mystatus'] = 'ERROR',
                activity['errors'] = dict(
                    step= 'blend',
                    message='ERROR {}'.format(os.path.basename(filename))
                )
                services.put_item_kinesis(activity)
                return

        # Build the raster to store the output images.
        width = profile['width']
        height = profile['height']

        # STACK will be generated in memory
        stack_raster = numpy.full((height, width), dtype=profile['dtype'], fill_value=nodata)

        # create file to save count no cloud
        build_cnc = activity['bands'][0] == band
        if build_cnc:
            cloud_cloud_file = '/tmp/cnc.tif'
            count_cloud_dataset = rasterio.open(cloud_cloud_file, mode='w', **profile)

        with MemoryFile() as medianfile:
            with medianfile.open(**profile) as mediandataset:
                for _, window in tilelist:
                    # Build the stack to store all images as a masked array.
                    # At this stage the array will contain the masked data
                    stackMA = numpy.ma.zeros((numscenes, window.height, window.width), dtype=numpy.int16)

                    notdonemask = numpy.ones(shape=(window.height,window.width),dtype=numpy.bool_)

                    row_offset = window.row_off + window.height
                    col_offset = window.col_off + window.width

                    # For all pair (quality,band) scenes
                    for order in range(numscenes):
                        ssrc = bandlist[order]
                        msrc = masklist[order]
                        raster = ssrc.read(1, window=window)
                        mask = msrc.read(1, window=window)
                        # Mask valid data (0 and 1) as True
                        mask[mask < 2] = 1
                        # Mask cloud/snow/shadow/no-data as False
                        mask[mask >= 2] = 0
                        # Ensure that Raster nodata value (-9999 maybe) is set to False
                        mask[raster == nodata] = 0

                        # Create an inverse mask value in order to pass to numpy masked array
                        # True => nodata
                        bmask = numpy.invert(mask.astype(numpy.bool_))

                        # Use the mask to mark the fill (0) and cloudy (2) pixels
                        stackMA[order] = numpy.ma.masked_where(bmask, raster)

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
                            stack_raster[window.row_off: row_offset, window.col_off: col_offset][where_intersec] = \
                            raster[where_intersec]

                        # Identify what is needed to stack, based in Array 2d bool
                        todomask = notdonemask * numpy.invert(bmask)

                        # Find all positions where valid data matches.
                        clear_not_done_pixels = numpy.where(numpy.logical_and(todomask, mask.astype(numpy.bool)))

                        # Override the STACK Raster with valid data.
                        stack_raster[window.row_off: row_offset, window.col_off: col_offset][clear_not_done_pixels] = \
                        raster[clear_not_done_pixels]

                        # Update what was done.
                        notdonemask = notdonemask * bmask

                    median_raster = numpy.ma.median(stackMA, axis=0).data
                    median_raster[notdonemask.astype(numpy.bool_)] = nodata
                    mediandataset.write(median_raster.astype(profile['dtype']), window=window, indexes=1)

                    if build_cnc:
                        count_raster = numpy.ma.count(stackMA, axis=0)
                        count_cloud_dataset.write(count_raster.astype(profile['dtype']), window=window, indexes=1)

                if band != activity['quality_band']:
                    mediandataset.nodata = nodata
                mediandataset.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
                mediandataset.update_tags(ns='rio_overview', resampling='nearest')

            services.upload_fileobj_S3(medianfile, activity['MEDfile'], {'ACL': 'public-read'}, bucket_name=bucket_name)

        # Close all input dataset
        for order in range(numscenes):
            bandlist[order].close()
            masklist[order].close()

        # Evaluate cloudcover
        cloudcover = 100. * ((height * width - numpy.count_nonzero(stack_raster)) / (height * width))
        activity['cloudratio'] = int(cloudcover)
        activity['raster_size_y'] = height
        activity['raster_size_x'] = width

        # Upload the CNC dataset
        if build_cnc:
            count_cloud_dataset.close()
            count_cloud_dataset = None

            key_cnc_med = '_'.join(activity['MEDfile'].split('_')[:-1]) + '_cnc.tif'
            key_cnc_stk = '_'.join(activity['STKfile'].split('_')[:-1]) + '_cnc.tif'
            services.upload_file_S3(cloud_cloud_file, key_cnc_med, {'ACL': 'public-read'}, bucket_name=bucket_name)
            services.upload_file_S3(cloud_cloud_file, key_cnc_stk, {'ACL': 'public-read'}, bucket_name=bucket_name)
            os.remove(cloud_cloud_file)

        # Create and upload the STACK dataset
        with MemoryFile() as memfile:
            with memfile.open(**profile) as ds_stack:
                if band != activity['quality_band']:
                    ds_stack.nodata = nodata
                ds_stack.write_band(1, stack_raster)
                ds_stack.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
                ds_stack.update_tags(ns='rio_overview', resampling='nearest')
            services.upload_fileobj_S3(memfile, activity['STKfile'], {'ACL': 'public-read'}, bucket_name=bucket_name)

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
# PUBLISH
###############################
def next_publish(services, blendactivity):
    # Fill the publish activity from blend activity
    publishactivity = {}
    for key in ['datacube','satellite','datasets','bands','quicklook','xmin','ymax','srs','tileid','start','end', \
        'dirname', 'cloudratio', 'raster_size_x', 'raster_size_y', 'chunk_size_x', 'chunk_size_y', 'bucket_name',
        'quality_band']:
        publishactivity[key] = blendactivity.get(key)
    publishactivity['action'] = 'publish'

    # Create  dynamoKey for the publish activity
    publishactivity['dynamoKey'] = encode_key(publishactivity, ['action','datacube','tileid','start','end'])

    # Get information from blended bands
    response = services.get_activities_by_key(blendactivity['dynamoKey'])

    items = response['Items']
    publishactivity['scenes'] = {}
    publishactivity['blended'] = {}
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
                publishactivity['scenes'][datedataset]['raster_size_x'] = scene.get('raster_size_x')
                publishactivity['scenes'][datedataset]['raster_size_y'] = scene.get('raster_size_y')
                publishactivity['scenes'][datedataset]['block_size'] = scene.get('block_size')
            publishactivity['scenes'][datedataset]['ARDfiles'][band] = scene['ARDfiles'][band]

        # Get blended files
        publishactivity['blended'][band] = {}
        publishactivity['blended'][band]['MEDfile'] = activity['MEDfile']
        publishactivity['blended'][band]['STKfile'] = activity['STKfile']

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
        warped_cube = '_'.join(activity['datacube'].split('_')[0:2])

        # Generate quicklooks for CUBES (MEDIAN, STACK ...)
        qlbands = activity['quicklook'].split(',')
        for function in ['MED', 'STK']:
            cube_id = get_cube_id(activity['datacube'], function)
            general_scene_id = '{}_{}_{}_{}'.format(
                cube_id, activity['tileid'], activity['start'], activity['end'])

            qlfiles = []
            for band in qlbands:
                qlfiles.append(prefix + activity['blended'][band][function + 'file'])

            pngname = generateQLook(general_scene_id, qlfiles)
            dirname_ql = activity['dirname'].replace(
                '{}/'.format(warped_cube), '{}/'.format(cube_id))
            if pngname is None:
                raise Exception('publish - Error generateQLook for {}'.format(general_scene_id))
            s3pngname = os.path.join(dirname_ql, '{}_{}'.format(activity['start'], activity['end']), os.path.basename(pngname))
            services.upload_file_S3(pngname, s3pngname, {'ACL': 'public-read'}, bucket_name=bucket_name)
            os.remove(pngname)

        # Generate quicklooks for all ARD scenes (WARPED)
        for datedataset in activity['scenes']:
            scene = activity['scenes'][datedataset]

            cube_id = get_cube_id(activity['datacube'])
            general_scene_id = '{}_{}_{}'.format(
                cube_id, activity['tileid'], str(scene['date'])[0:10])
            qlfiles = []
            for band in qlbands:
                filename = os.path.join(prefix + activity['dirname'], str(scene['date'])[0:10], scene['ARDfiles'][band])
                qlfiles.append(filename)

            pngname = generateQLook(general_scene_id, qlfiles)
            if pngname is None:
                raise Exception('publish - Error generateQLook for {}'.format(general_scene_id))
            s3pngname = os.path.join(activity['dirname'], str(scene['date'])[0:10], os.path.basename(pngname))
            services.upload_file_S3(pngname, s3pngname, {'ACL': 'public-read'}, bucket_name=bucket_name)
            os.remove(pngname)

        # register collection_items and assets in DB (MEDIAN, STACK ...)
        for function in ['MED', 'STK']:
            cube_id = '{}_{}'.format(activity['datacube'], function)
            cube = Collection.query().filter(
                Collection.id == cube_id
            ).first()
            if not cube:
                raise Exception('cube {} not found!'.format(cube_id))

            general_scene_id = '{}_{}_{}_{}'.format(
                cube_id, activity['tileid'], activity['start'], activity['end'])

            with db.session.begin_nested():
                # delete collection_items and assets if exists
                Asset.query().filter(Asset.collection_item_id == general_scene_id).delete()
                CollectionItem.query().filter(CollectionItem.id == general_scene_id).delete()

                # insert 'collection_item'
                range_date = '{}_{}'.format(activity['start'], activity['end'])
                png_name = '{}.png'.format(general_scene_id)
                dirname_ql = activity['dirname'].replace(
                    '{}/'.format(warped_cube), '{}/'.format(cube_id))
                s3_pngname = os.path.join(dirname_ql, range_date, png_name)
                CollectionItem(
                    id=general_scene_id,
                    collection_id=cube_id,
                    grs_schema_id=cube.grs_schema_id,
                    tile_id=activity['tileid'],
                    item_date=activity['start'],
                    composite_start=activity['start'],
                    composite_end=activity['end'],
                    quicklook='{}/{}'.format(bucket_name, s3_pngname),
                    cloud_cover=activity['cloudratio'],
                    scene_type=function,
                    compressed_file=None
                ).save(commit=False)

                # insert 'assets'
                bands_by_cube = Band.query().filter(
                    Band.collection_id == cube_id
                ).all()
                for band in activity['bands']:
<<<<<<< HEAD
                    if band == activity['quality_band']:
=======
                    if band == activity['quality_band'] and function != 'STK':
>>>>>>> a5a4b6d802de4c26ffcfd134caa349f03a66ecac
                        continue
                    band_id = list(filter(lambda b: str(b.common_name) == band, bands_by_cube))
                    if not band_id:
                        raise Exception('band {} not found!'.format(band))

                    Asset(
                        collection_id=cube_id,
                        band_id=band_id[0].id,
                        grs_schema_id=cube.grs_schema_id,
                        tile_id=activity['tileid'],
                        collection_item_id=general_scene_id,
                        url='{}/{}'.format(bucket_name, activity['blended'][band][function + 'file']),
                        source=None,
                        raster_size_x=activity['raster_size_x'],
                        raster_size_y=activity['raster_size_y'],
                        raster_size_t=1,
                        chunk_size_x=activity['chunk_size_x'],
                        chunk_size_y=activity['chunk_size_y'],
                        chunk_size_t=1
                    ).save(commit=False)
            db.session.commit()

        # Register all ARD scenes - WARPED Collection
        for datedataset in activity['scenes']:
            scene = activity['scenes'][datedataset]

            cube_id = get_cube_id(activity['datacube'])
            cube = Collection.query().filter(
                Collection.id == cube_id
            ).first()
            if not cube:
                raise Exception('cube {} not found!'.format(cube_id))

            general_scene_id = '{}_{}_{}'.format(
                cube_id, activity['tileid'], str(scene['date'])[0:10])

            with db.session.begin_nested():
                # delete 'assets' and 'collection_items' if exists
                Asset.query().filter(Asset.collection_item_id == general_scene_id).delete()
                CollectionItem.query().filter(CollectionItem.id == general_scene_id).delete()

                # insert 'collection_item'
                pngname = '{}.png'.format(general_scene_id)
                s3pngname = os.path.join(activity['dirname'], str(scene['date'])[0:10], pngname)
                CollectionItem(
                    id=general_scene_id,
                    collection_id=cube_id,
                    grs_schema_id=cube.grs_schema_id,
                    tile_id=activity['tileid'],
                    item_date=scene['date'],
                    composite_start=scene['date'],
                    composite_end=scene['date'],
                    quicklook='{}/{}'.format(bucket_name, s3pngname),
                    cloud_cover=int(scene['cloudratio']),
                    scene_type='WARPED',
                    compressed_file=None
                ).save(commit=False)

                # insert 'assets'
                bands_by_cube = Band.query().filter(
                    Band.collection_id == cube_id
                ).all()
                for band in activity['bands']:
                    if band not in scene['ARDfiles']:
                        raise Exception('publish - problem - band {} not in scene[files]'.format(band))
                    band_id = list(filter(lambda b: str(b.common_name) == band, bands_by_cube))
                    if not band_id:
                        raise Exception('band {} not found!'.format(band))

                    raster_size_x = scene['raster_size_x'] if scene.get('raster_size_x') else activity.get('raster_size_x')
                    raster_size_y = scene['raster_size_y'] if scene.get('raster_size_y') else activity.get('raster_size_y')
                    block_size = scene['block_size'] if scene.get('block_size') else activity.get('block_size')
                    Asset(
                        collection_id=cube_id,
                        band_id=band_id[0].id,
                        grs_schema_id=cube.grs_schema_id,
                        tile_id=activity['tileid'],
                        collection_item_id=general_scene_id,
                        url='{}/{}'.format(bucket_name, os.path.join(activity['dirname'], str(scene['date'])[0:10], scene['ARDfiles'][band])),
                        source=None,
                        raster_size_x=raster_size_x,
                        raster_size_y=raster_size_y,
                        raster_size_t=1,
                        chunk_size_x=block_size,
                        chunk_size_y=block_size,
                        chunk_size_t=1
                    ).save(commit=False)
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
