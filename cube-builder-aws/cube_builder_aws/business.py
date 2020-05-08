import json
import rasterio
from datetime import datetime
from geoalchemy2 import func
import sqlalchemy

from bdc_db.models.base_sql import BaseModel, db
from bdc_db.models import Collection, Band, CollectionTile, CollectionItem, Tile, \
    GrsSchema, RasterSizeSchema

from .utils.serializer import Serializer
from .utils.builder import get_date, get_cube_id
from .maestro import orchestrate, prepare_merge, \
    merge_warped, solo, blend, publish
from .services import CubeServices

class CubeBusiness:

    def __init__(self):
        self.score = {}

        self.services = CubeServices()

    def create_cube(self, params):
        params['composite_function_list'] = ['IDENTITY', 'STK', 'MED']

        # generate cubes metadata
        cubes_db = Collection.query().filter().all()
        cubes = []
        cubes_serealized = []
        for composite_function in params['composite_function_list']:
            c_function_id = composite_function.upper()
            raster_size_id = '{}-{}'.format(params['grs'], int(params['resolution']))
            cube_id = get_cube_id(params['datacube'], c_function_id)

            # add cube
            if not list(filter(lambda x: x.id == cube_id, cubes)) and not list(filter(lambda x: x.id == cube_id, cubes_db)):
                cube = Collection(
                    id=cube_id,
                    temporal_composition_schema_id=params['temporal_schema'] if c_function_id.upper() != 'IDENTITY' else 'Anull',
                    raster_size_schema_id=raster_size_id,
                    composite_function_schema_id=c_function_id,
                    grs_schema_id=params['grs'],
                    description=params['description'],
                    radiometric_processing=None,
                    geometry_processing=None,
                    sensor=None,
                    is_cube=True,
                    oauth_scope=params.get('oauth_scope', None),
                    license=params['license'],
                    bands_quicklook=','.join(params['bands_quicklook'])
                )
                cubes.append(cube)
                cubes_serealized.append(Serializer.serialize(cube))
        BaseModel.save_all(cubes)

        bands = []
        for cube in cubes:
            # save bands
            for band in params['bands']:
                band = band.strip()

                if (band == 'cnc' and cube.composite_function_schema_id == 'IDENTITY') or \
                    (band =='quality' and cube.composite_function_schema_id != 'IDENTITY'):
                    continue

                is_not_cloud = band != 'quality' and band != 'cnc'
                bands.append(Band(
                    name=band,
                    collection_id=cube.id,
                    min=0 if is_not_cloud else 0,
                    max=10000 if is_not_cloud else 255,
                    fill=-9999 if is_not_cloud else 0,
                    scale=0.0001 if is_not_cloud else 1,
                    data_type='int16' if is_not_cloud else 'Uint16',
                    common_name=band,
                    resolution_x=params['resolution'],
                    resolution_y=params['resolution'],
                    resolution_unit='m',
                    description='',
                    mime_type='image/tiff'
                ))
        BaseModel.save_all(bands)

        return cubes_serealized, 201

    def get_cube_status(self, datacube):
        activities = self.services.get_activities()
        activitiesCtrl = self.services.get_activities_ctrl()

        # STATUS
        items = [item for item in activitiesCtrl['Items'] if datacube in item['id']]
        not_done = [i for i in activities['Items'] if datacube in i['id'] and i['mystatus'] == 'NOTDONE']
        if len(not_done):
            return dict(
                finished = False,
                not_done = len(not_done)
            ), 200

        for item in items:
            items_by_id = [i for i in activities['Items'] if item['id'] in i['id']]
            if int(items_by_id[0]['totalInstancesToBeDone']) > int(item['mycount']):
                return dict(
                    finished = False,
                    not_done = int(items_by_id[0]['totalInstancesToBeDone']) - int(item['mycount'])
                ), 200

        # TIME
        acts = sorted(activities['Items'], key=lambda i: i['mystart'], reverse=True)
        start_date = get_date(acts[-1]['mystart'])
        end_date = get_date(acts[0]['mystart'])

        time = 0
        list_dates = []
        for a in acts:
            start = get_date(a['mystart'])
            end = get_date(a['myend'])
            if len(list_dates) == 0:
                time += (end - start).seconds
                list_dates.append({'s': start, 'e': end})
                continue

            time_by_act = 0
            i = 0
            for dates in list_dates:
                i += 1
                if dates['s'] < start < dates['e']:
                    value = (end - dates['e']).seconds
                    if value > 0 and value < time_by_act:
                        time_by_act = value

                elif dates['s'] < end < dates['e']:
                    value = (dates['s'] - start).seconds
                    if value > 0 and value < time_by_act:
                        time_by_act = value

                elif start > dates['e'] or end < dates['s']:
                    value = (end - start).seconds
                    if value < time_by_act or i == 1:
                        time_by_act = value

                elif start < dates['s'] or end > dates['e']:
                    time_by_act = 0

            time += time_by_act
            list_dates.append({'s': start, 'e': end})

        time_str = '{} h {} m {} s'.format(
            int(time / 60 / 60), int(time / 60), (time % 60))

        return dict(
            finished = True,
            start_date = str(start_date),
            last_date = str(end_date),
            duration = time_str
        ), 200

    def start_process(self, params):
        cube_id = get_cube_id(params['datacube'], 'MED')
        tiles = params['tiles'].split(',')
        start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').strftime('%Y-%m-%d')
        end_date = datetime.strptime(params['end_date'], '%Y-%m-%d').strftime('%Y-%m-%d') \
            if params.get('end_date') else datetime.now().strftime('%Y-%m-%d')

        # verify cube info
        cube_infos = Collection.query().filter(
            Collection.id == cube_id
        ).first()
        if not cube_infos:
            return 'Cube not found!', 404

        # get bands list
        bands = Band.query().filter(
            Band.collection_id == get_cube_id(params['datacube'])
        ).all()
        bands_list = [band.name for band in bands]

        # items => old mosaic
        # orchestrate
        self.score['items'] = orchestrate(params['datacube'], cube_infos, tiles, start_date, end_date)

        # prepare merge
        prepare_merge(self, params['datacube'], params['collections'].split(','), bands_list,
            cube_infos.bands_quicklook, bands[0].resolution_x, bands[0].resolution_y, bands[0].fill,
            cube_infos.raster_size_schemas.raster_size_x, cube_infos.raster_size_schemas.raster_size_y,
            cube_infos.raster_size_schemas.chunk_size_x, cube_infos.grs_schema.crs)

        return 'Succesfully', 201

    def continue_process_stream(self, params_list):
        params = params_list[0]
        if 'channel' in params and params['channel'] == 'kinesis':
            solo(self, params_list)

        # dispatch MERGE
        elif params['action'] == 'merge':
            merge_warped(self, params)

        # dispatch BLEND
        elif params['action'] == 'blend':
            blend(self, params)

        # dispatch PUBLISH
        elif params['action'] == 'publish':
            publish(self, params)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": 'Succesfully'
            }),
        }

    def create_grs(self, name, description, projection, meridian, degreesx, degreesy, bbox):
        bbox = bbox.split(',')
        bbox_obj = {
            "w": float(bbox[0]),
            "n": float(bbox[1]),
            "e": float(bbox[2]),
            "s": float(bbox[3])
        }
        tilesrsp4 = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
        if projection == 'aea':
            tilesrsp4 = "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0={} +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs".format(meridian)
        elif projection == 'sinu':
            tilesrsp4 = "+proj=sinu +lon_0={0} +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs".format(0.)

        # Number of tiles and base tile
        numtilesx = int(360./degreesx)
        numtilesy = int(180./degreesy)
        hBase = numtilesx/2
        vBase = numtilesy/2
        print('genwrs - hBase {} vBase {}'.format(hBase,vBase))

        # Tile size in meters (dx,dy) at center of system (argsmeridian,0.)
        src_crs = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
        dst_crs = tilesrsp4
        xs = [(meridian - degreesx/2), (meridian + degreesx/2), meridian, meridian, 0.]
        ys = [0., 0., -degreesy/2, degreesy/2, 0.]
        out = rasterio.warp.transform(src_crs, dst_crs, xs, ys, zs=None)
        x1 = out[0][0]
        x2 = out[0][1]
        y1 = out[1][2]
        y2 = out[1][3]
        dx = x2-x1
        dy = y2-y1

        # Coordinates of WRS center (0.,0.) - top left coordinate of (hBase,vBase)
        xCenter =  out[0][4]
        yCenter =  out[1][4]
        # Border coordinates of WRS grid
        xMin = xCenter - dx*hBase
        yMax = yCenter + dy*vBase

        # Upper Left is (xl,yu) Bottom Right is (xr,yb)
        xs = [bbox_obj['w'], bbox_obj['e'], meridian, meridian]
        ys = [0., 0., bbox_obj['n'], bbox_obj['s']]
        out = rasterio.warp.transform(src_crs, dst_crs, xs, ys, zs=None)
        xl = out[0][0]
        xr = out[0][1]
        yu = out[1][2]
        yb = out[1][3]
        hMin = int((xl - xMin)/dx)
        hMax = int((xr - xMin)/dx)
        vMin = int((yMax - yu)/dy)
        vMax = int((yMax - yb)/dy)

        # Insert grid
        grs = GrsSchema.query().filter(
            GrsSchema.id == name
        ).first()
        if not grs:
            GrsSchema(
                id=name,
                description=description,
                crs=tilesrsp4
            ).save()

        tiles = []
        dst_crs = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
        src_crs = tilesrsp4
        for ix in range(hMin, hMax+1):
            x1 = xMin + ix*dx
            x2 = x1 + dx
            for iy in range(vMin,vMax+1):
                y1 = yMax - iy*dy
                y2 = y1 - dy
                # Evaluate the bounding box of tile in longlat
                xs = [x1,x2,x2,x1]
                ys = [y1,y1,y2,y2]
                out = rasterio.warp.transform(src_crs, dst_crs, xs, ys, zs=None)
                UL_lon = out[0][0]
                UL_lat = out[1][0]
                UR_lon = out[0][1]
                UR_lat = out[1][1]
                LR_lon = out[0][2]
                LR_lat = out[1][2]
                LL_lon = out[0][3]
                LL_lat = out[1][3]

                wkt_wgs84 = 'POLYGON(({} {},{} {},{} {},{} {},{} {}))'.format(
                    UL_lon, UL_lat,
                    UR_lon, UR_lat,
                    LR_lon, LR_lat,
                    LL_lon, LL_lat,
                    UL_lon, UL_lat)

                wkt = 'POLYGON(({} {},{} {},{} {},{} {},{} {}))'.format(
                    x1, y2,
                    x2, y2,
                    x2, y1,
                    x1, y1,
                    x1, y2)

                # Insert tile
                tiles.append(Tile(
                    id='{0:03d}{1:03d}'.format(ix, iy),
                    grs_schema_id=name,
                    geom_wgs84='SRID=4326;{}'.format(wkt_wgs84),
                    geom='SRID=0;{}'.format(wkt),
                    min_x=x1,
                    max_y=y1
                ))

        BaseModel.save_all(tiles)
        return 'Grid {} created with successfully'.format(name), 201

    def create_raster_size(self, grs_schema, resolution, chunk_size_x, chunk_size_y):
        tile = db.session() \
            .query(
                Tile, func.ST_Xmin(Tile.geom), func.ST_Xmax(Tile.geom),
                func.ST_Ymin(Tile.geom), func.ST_Ymax(Tile.geom)
            ).filter(
                Tile.grs_schema_id == grs_schema
            ).first()
        if not tile:
            'GRS not found!', 404

        # x = Xmax - Xmin || y = Ymax - Ymin
        raster_size_x = int(round((tile[2]-tile[1])/int(resolution),0))
        raster_size_y = int(round((tile[4]-tile[3])/int(resolution),0))

        raster_schema_id = '{}-{}'.format(grs_schema, resolution)

        raster_schema = RasterSizeSchema.query().filter(
            RasterSizeSchema.id == raster_schema_id
        ).first()
        if raster_schema:
            raster_schema.raster_size_x = raster_size_x
            raster_schema.raster_size_y = raster_size_y
            raster_schema.chunk_size_x = chunk_size_x
            raster_schema.chunk_size_y = chunk_size_y
            raster_schema.save()

        RasterSizeSchema(
            id=raster_schema_id,
            raster_size_x=raster_size_x,
            raster_size_y=raster_size_y,
            raster_size_t=1,
            chunk_size_x=chunk_size_x,
            chunk_size_y=chunk_size_y,
            chunk_size_t=1
        ).save()
        return 'Schema created with successfully', 201

    @classmethod
    def list_cubes(cls):
        """Retrieve the list of data cubes from Brazil Data Cube database."""
        cubes = Collection.query().filter(Collection.is_cube.is_(True)).all()

        return [Serializer.serialize(cube) for cube in cubes], 200

    @classmethod
    def get_cube(cls, cube_name: str):
        collection = Collection.query().filter(Collection.id == cube_name).first()

        if collection is None or not collection.is_cube:
            return 'Cube "{}" not found.'.format(cube_name), 404

        temporal = db.session.query(
            func.min(CollectionItem.composite_start).cast(sqlalchemy.String),
            func.max(CollectionItem.composite_end).cast(sqlalchemy.String)
        ).filter(CollectionItem.collection_id == collection.id).first()

        if temporal is None:
            temporal = []

        dump_collection = Serializer.serialize(collection)
        dump_collection['temporal'] = temporal

        return dump_collection, 200

    @classmethod
    def list_tiles(cls, cube_name: str):
        features = db.session.query(
                func.ST_AsGeoJSON(func.ST_SetSRID(Tile.geom_wgs84, 4326), 6, 3).cast(sqlalchemy.JSON)
            ).filter(
                CollectionItem.tile_id == Tile.id,
                CollectionItem.collection_id == cube_name
            ).all()

        return [feature[0] for feature in features], 200
