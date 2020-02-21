import json
import os
import numpy
import rasterio

from datetime import datetime
from geoalchemy2 import func
from sqlalchemy import or_ 
from rasterio.transform import Affine 
from rasterio.warp import reproject, Resampling, transform
from rasterio.merge import merge 
from rasterio.io import MemoryFile

from bdc_db.models.base_sql import BaseModel, db
from bdc_db.models import CollectionTile, CollectionItem, Tile, \
    Collection, Asset, Band

from .utils.builder import decode_periods, encode_key, \
    getMaskStats, getMask, generateQLook
from config import BUCKET_NAME


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
        for function in ['WARPED', 'STK', 'MED']:
            collection_tile = CollectionTile.query().filter(
                CollectionTile.collection_id == '{}_{}'.format(datacube, function),
                CollectionTile.grs_schema_id == cube_infos.grs_schema_id,
                CollectionTile.tile_id == tile
            ).first()
            if not collection_tile:
                collection_tiles.append(CollectionTile(
                    collection_id='{}_{}'.format(datacube, function),
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
    timeline = decode_periods(cube_start_date, end_date, temporal_schema.upper(), int(step))

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
                bbox = tiles_infos[tile][1].replace('LINESTRING(', '').replace(')', '') \
                    .replace(' ', ',')

                items[tile] = items.get(tile, {})
                items[tile]['bbox'] = bbox
                items[tile]['xmin'] = tiles_infos[tile][0].min_x
                items[tile]['ymax'] = tiles_infos[tile][0].max_y
                items[tile]['periods'] = items[tile].get('periods', {})

                item_id = '{}_{}_{}_{}'.format(cube_infos.id, tile, p_startdate, p_enddate)
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
                            'dirname': '{}/{}/{}_{}/'.format(datacube, tile, p_startdate, p_enddate)
                        }
    return items


def solo(self, activitylist):
    services = self.services

    for activity in activitylist:
        services.put_activity(activity)
        if activity['mystatus'] == 'DONE' and activity['action'] != 'publish':
            next_step(services, activity)


def next_step(services, activity):
    activitiesControlTableKey = activity['dynamoKey'].replace(activity['band'],'')
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
def check_merge(services, datacube, collections, mosaics, bands):
    activity = {}
    activity['action'] = 'merge'
    activity['datacube'] = datacube
    activity['datasets'] = collections
    activity['bands'] = bands

    check = {}
    # For all tiles
    for tileid in mosaics:
        activity['tileid'] = tileid
        
        # For all periods
        for periodkey in mosaics[tileid]['periods']:
            activity['start'] = mosaics[tileid]['periods'][periodkey]['composite_start']
            activity['end'] = mosaics[tileid]['periods'][periodkey]['composite_end']
            
            # For all bands and collections/datasets
            for band in activity['bands']:
                for _ in activity['datasets']:
                    key = encode_key(activity, ['action','datacube','tileid','start','end']) + band
                    response = services.get_activities(key)
                    if 'Items' not in response or len(response['Items']) == 0:
                        check[key] = 'NOTDONE'
                        continue

                    items = response['Items']
                    for item in items:
                        json_item = json.loads(item['activity'])
                        date = item['sk']
                        mystatus = item['mystatus']
                        if mystatus != 'DONE':
                            key = key+'_'+date
                            check[key] = {}
                            check[key]['status'] = mystatus
                            check[key]['links'] = json_item['links']
                            check[key]['ARDfile'] = json_item['ARDfile']
    return check

def prepare_merge(self, datacube, datasets, bands, quicklook, resx, resy, nodata, numcol, numlin, block_size, crs):
    services = self.services

    # Build the basics of the merge activity
    activity = {}
    activity['action'] = 'merge'
    activity['datacube'] = datacube
    activity['datasets'] = datasets
    activity['bands'] = bands
    activity['quicklook'] = quicklook
    activity['resx'] = resx
    activity['resy'] = resy
    activity['numcol'] = numcol
    activity['numlin'] = numlin
    activity['nodata'] = nodata
    activity['block_size'] = block_size
    activity['srs'] = crs

    # For all tiles
    for tileid in self.score['items']:
        if len(self.score['items']) != 1:
            self.params['tileid'] = tileid
            services.send_to_sqs(self.params)
            continue

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

            # Search all images
            self.score['items'][tileid]['periods'][periodkey]['scenes'] = services.search_STAC(activity)

            # Evaluate the number of dates, the number of scenes for each date and the total amount merges that will be done
            number_of_datasets_dates = 0
            band = activity['bands'][0]
            for dataset in self.score['items'][tileid]['periods'][periodkey]['scenes'][band].keys():
                for date in self.score['items'][tileid]['periods'][periodkey]['scenes'][band][dataset].keys():
                    number_of_datasets_dates += 1
            activity['instancesToBeDone'] = number_of_datasets_dates
            activity['totalInstancesToBeDone'] = number_of_datasets_dates*len(activity['bands'])

            # Reset mycount in activitiesControlTable
            activities_control_table_key = encode_key(activity, ['action','datacube','tileid','start','end'])
            services.put_control_table(activities_control_table_key, 0)

            # Save the activity in DynamoDB if no scenes are available
            if number_of_datasets_dates == 0:
                dynamo_key = encode_key(activity, ['action','datacube','tileid','start','end'])
                mylaunch = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                activity['dynamoKey'] = dynamo_key
                activity['sk'] = 'NODATE'
                activity['mystatus'] = 'NOSCENES'
                activity['mylaunch'] = mylaunch
                activity['mystart'] = 'XXXX-XX-XX'
                activity['myend'] = 'YYYY-YY-YY'
                activity['efficacy'] = '0'
                activity['cloudratio'] = '100'
                services.put_item_kinesis(activity)
                continue

            # Build each merge activity
            # For all bands
            for band in self.score['items'][tileid]['periods'][periodkey]['scenes']:
                activity['band'] = band

                # Create the dynamoKey for the activity in DynamoDB
                activity['dynamoKey'] = encode_key(activity,['action','datacube','tileid','start','end','band'])

                # For all datasets
                for dataset in self.score['items'][tileid]['periods'][periodkey]['scenes'][band]:
                    activity['dataset'] = dataset

                    # get resolution by dataset
                    if 'LC8SR' in dataset:
                        activity['resolution'] = '30'
                    elif dataset == 'CBERS-4_AWFI':
                        activity['resolution'] = '64'
                    elif dataset == 'CBERS-4_MUX':
                        activity['resolution'] = '20'
                    elif 'S2SR' in dataset:
                        activity['resolution'] = '10'
                    elif dataset == 'MOD13Q1':
                        activity['resolution'] = '231'
                    elif dataset == 'MYD13Q1':
                        activity['resolution'] = '231'

                    # For all dates
                    for date in self.score['items'][tileid]['periods'][periodkey]['scenes'][band][dataset]:
                        activity['date'] = date
                        activity['links'] = []

                        # Get all scenes that were acquired in the same date
                        for scene in self.score['items'][tileid]['periods'][periodkey]['scenes'][band][dataset][date]:
                            activity['links'].append(scene['link'])

                        # Continue filling the activity
                        activity['ARDfile'] = activity['dirname']+'{}_WARPED_{}_{}_{}.tif'.format(
                            activity['datacube'], activity['tileid'], date[0:10], band)
                        activity['sk'] = activity['date'] + activity['dataset']
                        activity['mylaunch'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        # Check if we have already done and no need to do it again
                        response = services.get_activity_item({'id': activity['dynamoKey'], 'sk': activity['sk'] })
                        if 'Item' in response:
                            if response['Item']['instancesToBeDone'] == activity['instancesToBeDone'] \
                                and response['Item']['mystatus'] == 'DONE' \
                                and services.s3fileExists(key=activity['ARDfile']):
                                next_step(services, activity)
                                continue
                        else:
                            activity['mystatus'] = 'NOTDONE'
                            activity['mystart'] = 'SSSS-SS-SS'
                            activity['myend'] = 'EEEE-EE-EE'
                            activity['efficacy'] = '0'
                            activity['cloudratio'] = '100'

                            # Send to queue to activate merge lambda
                            services.put_item_kinesis(activity)
                            services.send_to_sqs(activity)

def merge_warped(self, activity):
    print('==> start MERGE')
    services = self.services
    key = activity['ARDfile']

    mystart = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['mystart'] = mystart
    activity['mystatus'] = 'DONE'

    # If ARDfile already exists, update activitiesTable and chech if this merge is the last one for the mosaic
    if services.s3fileExists(key=key):
        efficacy = 0
        cloudratio = 100
        if activity['band'] == 'quality':
            with rasterio.open('{}{}'.format(services.prefix, key)) as src:
                mask = src.read(1)
                cloudratio, _, efficacy = getMaskStats(mask)

        # Update entry in DynamoDB
        activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        activity['efficacy'] = '{}'.format(int(efficacy))
        activity['cloudratio'] = '{}'.format(int(cloudratio))
        services.put_item_kinesis(activity)

        key = '{}activities/{}{}.json'.format(activity['dirname'], activity['dynamoKey'], activity['date'])
        services.save_file_S3(key, activity)
        return

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
    if band == 'quality':
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

                        if band != 'quality':
                            valid_data_scene = raster[raster != nodata]
                            raster_merge[raster != nodata] = valid_data_scene.reshape(numpy.size(valid_data_scene))
                        else:
                            raster_merge = raster_merge + raster * raster_mask
                            raster_mask[raster != nodata] = 0

                        if template is None:
                            template = dst.profile
                            if band != 'quality':
                                template['dtype'] = 'int16'
                                template['nodata'] = nodata


    # Evaluate cloud cover and efficacy if band is quality
    efficacy = 0
    cloudratio = 100
    if activity['band'] == 'quality':
        raster_merge, efficacy, cloudratio = getMask(raster_merge, activity['dataset'])
        template.update({'dtype': 'uint16'})

    # Save merged image on S3
    with MemoryFile() as memfile:
        template.update({
            'compress': 'LZW',
            'tiled': True,
            'blockxsize': block_size,
            'blockysize': block_size
        })  
        with memfile.open(**template) as riodataset:
            riodataset.nodata = nodata
            riodataset.write_band(1, raster_merge)
            riodataset.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
            riodataset.update_tags(ns='rio_overview', resampling='nearest')
        services.upload_fileobj_S3(memfile, key, {'ACL': 'public-read'})

    # Update entry in DynamoDB
    myend = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['myend'] = myend
    activity['efficacy'] = '{}'.format(int(efficacy))
    activity['cloudratio'] = '{}'.format(int(cloudratio))
    activity['raster_size_x'] = '{}'.format(numcol)
    activity['raster_size_y'] = '{}'.format(numlin)
    activity['block_size'] = '{}'.format(block_size)
    services.put_item_kinesis(activity)

    key = '{}activities/{}{}.json'.format(activity['dirname'], activity['dynamoKey'], activity['date'])
    services.save_file_S3(key, activity)


###############################
# BLEND
###############################
def check_blend(services, datacube, collections, bands, mosaics):
    # Build the basics of the activity
    activity = {}
    activity['action'] = 'blend'
    activity['datacube'] = datacube
    activity['datasets'] = collections
    activity['bands'] = bands
    
    check = {}
    # For all tiles
    for tileid in mosaics:
        activity['tileid'] = tileid

        # For all periods
        for periodkey in mosaics[tileid]['periods']:
            activity['start'] = mosaics[tileid]['periods'][periodkey]['start']
            activity['end'] = mosaics[tileid]['periods'][periodkey]['end']
            for band in activity['bands']:
                for _ in activity['datasets']:
                    key = encode_key(activity, ['action','datacube','tileid','start','end'])
                    response = services.get_activities(key)
                    if 'Items' not in response or len(response['Items']) == 0:
                        return

                    items = response['Items']
                    for item in items:
                        jitem = json.loads(item['activity'])
                        band = item['sk']
                        mystatus = item['mystatus']
                        if mystatus != 'DONE':
                            key = key+'_'+band
                            check[key] = {}
                            check[key]['status'] = mystatus
                            check[key]['scenes'] = jitem['scenes']
                            check[key]['MEDfile'] = jitem['MEDfile']
                            check[key]['STKfile'] = jitem['STKfile']
    return check

def prepare_blend(self, datacube, datasets, bands, quicklook):
    services = self.services
    # Build the basics of the dynamoKey for a synthetic previous merge activity 
    mergeactivity = {}
    mergeactivity['datacube'] = datacube
    mergeactivity['datasets'] = datasets
    mergeactivity['bands'] = bands
    mergeactivity['quicklook'] = quicklook
    mergeactivity['action'] = 'merge'

    # For all tiles
    for tileid in self.score['items']:
        if len(self.score['items']) != 1:
            self.params['tileid'] = tileid
            services.send_to_sqs(self.params)
            continue

        mergeactivity['tileid'] = tileid
        mergeactivity['bbox'] = self.score['items'][tileid]['bbox']
        mergeactivity['xmin'] = self.score['items'][tileid]['xmin']
        mergeactivity['ymax'] = self.score['items'][tileid]['ymax']
        
        # For all periods
        for periodkey in self.score['items'][tileid]['periods']:
            mergeactivity['start'] = self.score['items'][tileid]['periods'][periodkey]['composite_start']
            mergeactivity['end'] = self.score['items'][tileid]['periods'][periodkey]['composite_end']
            mergeactivity['dirname'] = self.score['items'][tileid]['periods'][periodkey]['dirname']
            next_blend(services, mergeactivity)

def next_blend(services, mergeactivity):
    # Fill the blendactivity from mergeactivity
    blendactivity = {}	
    blendactivity['action'] = 'blend'
    for key in ['datacube','datasets','bands','quicklook','xmin','ymax','srs','tileid','start','end','dirname','nodata']:
        blendactivity[key] = mergeactivity[key]
    blendactivity['totalInstancesToBeDone'] = len(blendactivity['bands'])-1
    
    # Create  dynamoKey for the blendactivity record
    blendactivity['dynamoKey'] = encode_key(blendactivity, ['action','datacube','tileid','start','end'])

    # Fill the blendactivity fields with data for quality band from the DynamoDB merge records
    blendactivity['scenes'] = {}
    blendactivity['band'] = 'quality'
    mergeactivity['band'] = 'quality'
    _ = fill_blend(services, mergeactivity, blendactivity)

    # Reset mycount in  activitiesControlTable
    activitiesControlTableKey = blendactivity['dynamoKey']
    services.put_control_table(activitiesControlTableKey, 0)

    # If no quality file was found for this tile/period, register it in DynamoDB and go on
    if blendactivity['instancesToBeDone'] == 0:
        blendactivity['sk'] = 'ALBANDS'
        blendactivity['mystatus'] = 'NOSCENES'
        blendactivity['mystart'] = 'SSSS-SS-SS'
        blendactivity['myend'] = 'EEEE-EE-EE'
        blendactivity['efficacy'] = '0'
        blendactivity['cloudratio'] = '100'
        mylaunch = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        blendactivity['mylaunch'] = mylaunch
        services.put_item_kinesis(blendactivity)
        return False
        
    # Fill the blendactivity fields with data for the other bands from the DynamoDB merge records (quality band is not a blend entry in DynamoDB)
    for band in blendactivity['bands']:
        if band == 'quality': continue

        mergeactivity['band'] = band
        blendactivity['band'] = band
        blendactivity['sk'] = band
        _ = fill_blend(services, mergeactivity, blendactivity)

        # Check if we are doing it again and if we have to do it because a different number of ARDfiles is present
        response = services.get_activity_item({'id': blendactivity['dynamoKey'], 'sk': band })

        if 'Item' in response \
                and response['Item']['mystatus'] == 'DONE' \
                and response['Item']['instancesToBeDone'] == blendactivity['instancesToBeDone'] \
                and services.s3fileExists(key=blendactivity['MEDfile']) \
                and services.s3fileExists(key=blendactivity['STKfile']):
            blendactivity['mystatus'] = 'DONE'
            next_step(services, blendactivity)
            continue

        # Blend has not been performed, do it
        blendactivity['mystatus'] = 'NOTDONE'
        blendactivity['mystart'] = 'SSSS-SS-SS'
        blendactivity['myend'] = 'EEEE-EE-EE'
        blendactivity['efficacy'] = '0'
        blendactivity['cloudratio'] = '100'
        mylaunch = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        blendactivity['mylaunch'] = mylaunch
        
        # Create an entry in dynamoDB for each band blend activity (quality band is not an entry in DynamoDB)
        key = '{}activities/{}.json'.format(blendactivity['dirname'], blendactivity['dynamoKey'])
        services.save_file_S3(key, blendactivity)
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
    dynamoKey = encode_key(mergeactivity, ['action','datacube','tileid','start','end','band'])

    # Query dynamoDB to get all merged 
    response = services.get_activities(dynamoKey)
    if 'Items' not in response or len(response['Items']) == 0:
        return False
    items = response['Items']

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
    if band != 'quality':
        for function in ['MED', 'STK']:
            cube_id = '{}_{}'.format(blendactivity['datacube'], function)
            blendactivity['{}file'.format(function)] = '{0}/{1}/{2}_{3}/{0}_{1}_{2}_{3}_{4}.tif'.format(
                cube_id, blendactivity['tileid'], blendactivity['start'], blendactivity['end'], band)
    return True

def blend(self, activity):
    print('==> start BLEND')
    services = self.services

    activity['mystart'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['sk'] = activity['band']
    band = activity['band']
    numscenes = len(activity['scenes'])

    # Check if band ARDfiles are in activity
    for datedataset in activity['scenes']:
        if band not in activity['scenes'][datedataset]['ARDfiles']:
            activity['mystatus'] = 'ERROR band {}'.format(band)
            services.put_item_kinesis(activity)
            return

    # Get basic information (profile) of input files
    keys = list(activity['scenes'].keys())
    filename = os.path.join(services.prefix + activity['dirname'], activity['scenes'][keys[0]]['ARDfiles'][band])
    tilelist = []
    profile = None
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
        filename = os.path.join(services.prefix + activity['dirname'], scene['ARDfiles']['quality'])
        try:
            masklist.append(rasterio.open(filename))
        except:
            activity['mystatus'] = 'ERROR {}'.format(os.path.basename(filename))
            services.put_item_kinesis(activity)
            return

        # BANDS
        filename = os.path.join(services.prefix + activity['dirname'], scene['ARDfiles'][band])
        try:
            bandlist.append(rasterio.open(filename))
        except:
            activity['mystatus'] = 'ERROR {}'.format(os.path.basename(filename))
            services.put_item_kinesis(activity)
            return

    # Build the raster to store the output images.		
    width = profile['width']
    height = profile['height']

    # STACK will be generated in memory
    stackRaster = numpy.zeros((height,width), dtype=profile['dtype'])
    maskRaster = numpy.ones((height,width), dtype=profile['dtype'])

    with MemoryFile() as medianfile:
        with medianfile.open(**profile) as mediandataset:
            for _, window in tilelist:
                # Build the stack to store all images as a masked array. At this stage the array will contain the masked data	
                stackMA = numpy.ma.zeros((numscenes, window.height, window.width), dtype=numpy.int16)

                notdonemask = numpy.ones(shape=(window.height,window.width),dtype=numpy.bool_)

                # For all pair (quality,band) scenes 
                for order in range(numscenes):
                    ssrc = bandlist[order]
                    msrc = masklist[order]
                    raster = ssrc.read(1, window=window)
                    mask = msrc.read(1, window=window)
                    mask[mask != 1] = 0

                    # True => nodata
                    bmask = numpy.invert(mask.astype(numpy.bool_))

                    # Use the mask to mark the fill (0) and cloudy (2) pixels
                    stackMA[order] = numpy.ma.masked_where(bmask, raster)

                    # Evaluate the STACK image
                    # Pixels that have been already been filled by previous rasters will be masked in the current raster
                    maskRaster[window.row_off : window.row_off+window.height, window.col_off : window.col_off+window.width] *= bmask.astype(profile['dtype'])
                    
                    raster[raster == -9999] = 0
                    todomask = notdonemask * numpy.invert(bmask)
                    notdonemask = notdonemask * bmask
                    stackRaster[window.row_off : window.row_off+window.height, window.col_off : window.col_off+window.width] += (todomask * raster.astype(profile['dtype']))

                medianRaster = numpy.ma.median(stackMA, axis=0).data
                medianRaster[notdonemask.astype(numpy.bool_)] = -9999
                mediandataset.write(medianRaster.astype(profile['dtype']), window=window, indexes=1)

            stackRaster[maskRaster.astype(numpy.bool_)] = -9999

            if band != 'quality':
                mediandataset.nodata = -9999
            mediandataset.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
            mediandataset.update_tags(ns='rio_overview', resampling='nearest')
        services.upload_fileobj_S3(medianfile, activity['MEDfile'], {'ACL': 'public-read'})

    # Close all input dataset
    for order in range(numscenes):
        bandlist[order].close()
        masklist[order].close()

    # Evaluate cloudcover
    cloudcover = 100.*((height*width - numpy.count_nonzero(stackRaster))/(height*width))
    activity['cloudratio'] = int(cloudcover)
    activity['raster_size_y'] = height
    activity['raster_size_x'] = width

    # Create and upload the STACK dataset
    with MemoryFile() as memfile:
        with memfile.open(**profile) as ds_stack:
            if band != 'quality':
                ds_stack.nodata = -9999
            ds_stack.write_band(1, stackRaster)
            ds_stack.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
            ds_stack.update_tags(ns='rio_overview', resampling='nearest')
        services.upload_fileobj_S3(memfile, activity['STKfile'], {'ACL': 'public-read'})

    # Update status and end time in DynamoDB
    activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['mystatus'] = 'DONE'
    services.put_item_kinesis(activity)


###############################
# PUBLISH
###############################
def prepare_publish(self, datacube, datasets, bands, quicklook):
    services = self.services

    # Build the basics of the dynamoKey for a synthetic previous blend activity 
    blendactivity = {}
    blendactivity['datacube'] = datacube
    blendactivity['datasets'] = datasets
    blendactivity['bands'] = bands
    blendactivity['quicklook'] = quicklook
    blendactivity['action'] = 'blend'

    # For all tiles
    for tileid in self.score['items']:
        blendactivity['tileid'] = tileid
        for key in ['xmin','ymax']:
            blendactivity[key] = self.score['items'][tileid][key]

        # For all periods
        for periodkey in self.score['items'][tileid]['periods']:
            blendactivity['start'] = self.score['items'][tileid]['periods'][periodkey]['composite_start']
            blendactivity['end'] = self.score['items'][tileid]['periods'][periodkey]['composite_end']
            blendactivity['dirname'] = self.score['items'][tileid]['periods'][periodkey]['dirname']
            blendactivity['dynamoKey'] = encode_key(blendactivity, ['action','datacube','tileid','start','end'])
            next_publish(services, blendactivity)

def next_publish(services, blendactivity):
    # Fill the publish activity from blend activity
    publishactivity = {}
    for key in ['datacube','datasets','bands','quicklook','xmin','ymax','srs','tileid','start','end', \
        'dirname', 'cloudratio', 'raster_size_x', 'raster_size_y', 'chunk_size_x', 'chunk_size_y']:
        publishactivity[key] = blendactivity.get(key)
    publishactivity['action'] = 'publish'

    # Create  dynamoKey for the publish activity 
    publishactivity['dynamoKey'] = encode_key(publishactivity, ['action','datacube','tileid','start','end'])

    # Get information from blended bands
    response = services.get_activities(blendactivity['dynamoKey'])

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
                publishactivity['scenes'][datedataset]['ARDfiles']['quality'] = scene['ARDfiles']['quality']
                publishactivity['scenes'][datedataset]['date'] = scene['date']
                publishactivity['scenes'][datedataset]['dataset'] = scene['dataset']
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
    print('==> start PUBLISH')
    services = self.services

    activity['mystart'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')    
    # Generate quicklooks for CUBES (MEDIAN, STACK ...) 
    qlbands = activity['quicklook'].split(',')
    for function in ['MED', 'STK']:
        cube_id = '{}_{}'.format(activity['datacube'], function)
        general_scene_id = '{}_{}_{}_{}'.format(
            cube_id, activity['tileid'], activity['start'], activity['end'])

        qlfiles = []
        for band in qlbands:
            qlfiles.append(services.prefix + activity['blended'][band][function + 'file'])

        pngname = generateQLook(general_scene_id, qlfiles)
        dirname_ql = activity['dirname'].replace(
            '{}/'.format(activity['datacube']), '{}/'.format(cube_id))
        if pngname is None:
            print('publish - Error generateQLook for {}'.format(general_scene_id))
            return False
        s3pngname = os.path.join(dirname_ql, os.path.basename(pngname))
        services.upload_file_S3(pngname, s3pngname, {'ACL': 'public-read'})
        os.remove(pngname)

    # Generate quicklooks for all ARD scenes (WARPED)
    for datedataset in activity['scenes']:
        scene = activity['scenes'][datedataset]

        general_scene_id = '{}_WARPED_{}_{}'.format(
            activity['datacube'], activity['tileid'], str(scene['date'])[0:10])
        qlfiles = []
        for band in qlbands:
            filename = os.path.join(services.prefix + activity['dirname'], scene['ARDfiles'][band])
            qlfiles.append(filename)

        pngname = generateQLook(general_scene_id, qlfiles)
        if pngname is None:
            print('publish - Error generateQLook for {}'.format(general_scene_id))
            return False
        s3pngname = os.path.join(activity['dirname'], os.path.basename(pngname))
        services.upload_file_S3(pngname, s3pngname, {'ACL': 'public-read'})
        os.remove(pngname)

    # register collection_items and assets in DB (MEDIAN, STACK ...)
    for function in ['MED', 'STK']:
        cube_id = '{}_{}'.format(activity['datacube'], function)
        cube = Collection.query().filter(
            Collection.id == cube_id
        ).first()
        if not cube:
            print('cube {} not found!'.format(cube_id))
            continue

        general_scene_id = '{}_{}_{}_{}'.format(
            cube_id, activity['tileid'], activity['start'], activity['end'])

        # delete collection_items and assets if exists
        assets = Asset.query().filter(
            Asset.collection_item_id == general_scene_id
        ).all()
        for asset in assets:
            db.session().delete(asset)
            db.session().commit()

        coll_item = CollectionItem.query().filter(
            CollectionItem.id == general_scene_id
        ).first()
        if coll_item:
            db.session().delete(coll_item)
            db.session().commit()

        # insert 'collection_item'
        pngname = '{}.png'.format(general_scene_id)
        dirname_ql = activity['dirname'].replace(
            '{}/'.format(activity['datacube']), '{}/'.format(cube_id))
        s3pngname = os.path.join(dirname_ql, pngname)
        CollectionItem(
            id=general_scene_id,
            collection_id=cube_id,
            grs_schema_id=cube.grs_schema_id,
            tile_id=activity['tileid'],
            item_date=activity['start'],
            composite_start=activity['start'],
            composite_end=activity['end'],
            quicklook='{}/{}'.format(BUCKET_NAME, s3pngname),
            cloud_cover=activity['cloudratio'],
            scene_type=function,
            compressed_file=None
        ).save()

        # insert 'assets'
        bands_by_cube = Band.query().filter(
            Band.collection_id == cube_id
        ).all()
        for band in activity['bands']:
            if band == 'quality': 
                continue
            band_id = list(filter(lambda b: str(b.common_name) == band, bands_by_cube))
            if not band_id:
                print('band {} not found!'.format(band))
                continue

            Asset(
                collection_id=cube_id,
                band_id=band_id[0].id,
                grs_schema_id=cube.grs_schema_id,
                tile_id=activity['tileid'],
                collection_item_id=general_scene_id,
                url='{}/{}'.format(BUCKET_NAME, activity['blended'][band][function + 'file']),
                source=None,
                raster_size_x=activity['raster_size_x'],
                raster_size_y=activity['raster_size_y'],
                raster_size_t=1,
                chunk_size_x=activity['chunk_size_x'],
                chunk_size_y=activity['chunk_size_y'],
                chunk_size_t=1
            ).save()

    # Register all ARD scenes - WARPED Collection
    for datedataset in activity['scenes']:
        scene = activity['scenes'][datedataset]

        cube_id = '{}_WARPED'.format(activity['datacube'])
        cube = Collection.query().filter(
            Collection.id == cube_id
        ).first()
        if not cube:
            print('cube {} not found!'.format(cube_id))
            continue

        general_scene_id = '{}_{}_{}'.format(
            cube_id, activity['tileid'], str(scene['date'])[0:10])

        # delete 'assets' and 'collection_items' if exists
        assets = Asset.query().filter(
            Asset.collection_item_id == general_scene_id
        ).all()
        for asset in assets:
            db.session().delete(asset)
            db.session().commit()

        coll_item = CollectionItem.query().filter(
            CollectionItem.id == general_scene_id
        ).first()
        if coll_item:
            db.session().delete(coll_item)
            db.session().commit()

        # insert 'collection_item'
        pngname = '{}.png'.format(general_scene_id)
        s3pngname = os.path.join(activity['dirname'], pngname)
        CollectionItem(
            id=general_scene_id,
            collection_id=cube_id,
            grs_schema_id=cube.grs_schema_id,
            tile_id=activity['tileid'],
            item_date=scene['date'],
            composite_start=activity['start'],
            composite_end=activity['end'],
            quicklook='{}/{}'.format(BUCKET_NAME, s3pngname),
            cloud_cover=int(scene['cloudratio']),
            scene_type='WARPED',
            compressed_file=None
        ).save()

        # insert 'assets'
        bands_by_cube = Band.query().filter(
            Band.collection_id == cube_id
        ).all()
        for band in activity['bands']:
            if band not in scene['ARDfiles']:
                print('publish - problem - band {} not in scene[files]'.format(band))
                continue
            band_id = list(filter(lambda b: str(b.common_name) == band, bands_by_cube))
            if not band_id:
                print('band {} not found!'.format(band))
                continue

            Asset(
                collection_id=cube_id,
                band_id=band_id[0].id,
                grs_schema_id=cube.grs_schema_id,
                tile_id=activity['tileid'],
                collection_item_id=general_scene_id,
                url='{}/{}'.format(BUCKET_NAME, os.path.join(activity['dirname'], scene['ARDfiles'][band])),
                source=None,
                raster_size_x=int(scene['raster_size_x']),
                raster_size_y=int(scene['raster_size_y']),
                raster_size_t=1,
                chunk_size_x=int(scene['block_size']),
                chunk_size_y=int(scene['block_size']),
                chunk_size_t=1
            ).save()

    # Update status and end time in DynamoDB
    activity['myend'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    activity['mystatus'] = 'DONE'
    services.put_item_kinesis(activity)
    return True