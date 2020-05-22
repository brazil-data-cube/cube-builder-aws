import json
import rasterio
from datetime import datetime
from geoalchemy2 import func
import sqlalchemy
from sqlalchemy.sql import functions as db_functions

from bdc_db.models.base_sql import BaseModel, db
from bdc_db.models import Asset, Collection, Band, CollectionItem, Tile, \
    GrsSchema, RasterSizeSchema, TemporalCompositionSchema, CompositeFunctionSchema

from .utils.serializer import Serializer
from .utils.builder import get_date, get_cube_id, get_cube_parts
from .maestro import orchestrate, prepare_merge, \
    merge_warped, solo, blend, publish
from .services import CubeServices
from .utils.image import validate_merges

class CubeBusiness:

    def __init__(self, url_stac=None, bucket=None):
        self.score = {}

        self.services = CubeServices(url_stac, bucket)

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
        datacube_request = datacube

        # split and format datacube NAME
        parts_cube_name = get_cube_parts(datacube)
        irregular_datacube = '_'.join(parts_cube_name[:2])
        is_irregular = len(parts_cube_name) > 2
        datacube = '_'.join(get_cube_parts(datacube)[:3]) if is_irregular else irregular_datacube

        # STATUS
        acts_datacube = []
        not_done_datacube = 0
        if is_irregular:
            acts_datacube = self.services.get_activities_by_datacube(datacube)
            not_done_datacube = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', acts_datacube)))
        acts_irregular = self.services.get_activities_by_datacube(irregular_datacube)
        not_done_irregular = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', acts_irregular)))

        activities = acts_irregular + acts_datacube
        not_done = not_done_irregular + not_done_datacube
        if not_done:
            return dict(
                finished = False,
                done = len(activities) - not_done,
                not_done = not_done
            ), 200

        # TIME
        acts = sorted(activities, key=lambda i: i['mylaunch'], reverse=True)
        start_date = get_date(acts[-1]['mylaunch'])
        end_date = get_date(acts[0]['myend'])

        time = 0
        list_dates = []
        for a in acts:
            start = get_date(a['mylaunch'])
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

        quantity_coll_items = CollectionItem.query().filter(
            CollectionItem.collection_id == datacube_request
        ).count()

        return dict(
            finished = True,
            start_date = str(start_date),
            last_date = str(end_date),
            done = len(activities),
            duration = time_str,
            collection_item = quantity_coll_items
        ), 200

    def start_process(self, params):
        cube_id = get_cube_id(params['datacube'], 'MED')
        tiles = params['tiles']
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
        prepare_merge(self, params['datacube'], params['collections'].split(','), params['satellite'], bands_list,
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
            db.session.commit()
            return 'Schema created with successfully', 201

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

    def list_cubes(self):
        """Retrieve the list of data cubes from Brazil Data Cube database."""
        cubes = Collection.query().filter(Collection.is_cube.is_(True)).all()

        list_cubes = []
        for cube in cubes:
            cube_formated = Serializer.serialize(cube)
            not_done = 0
            if cube.composite_function_schema_id != 'IDENTITY':
                parts = get_cube_parts(cube.id)
                data_cube = '_'.join(parts[:3])
                activities = self.services.get_activities_by_datacube(data_cube)
                not_done = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', activities)))

            parts = get_cube_parts(cube.id)
            data_cube_identity = '_'.join(parts[:2])
            activities = self.services.get_activities_by_datacube(data_cube_identity)
            not_done_identity = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', activities)))

            sum_not_done = not_done + not_done_identity
            cube_formated['finished'] = sum_not_done == 0
            list_cubes.append(cube_formated)

        return list_cubes, 200

    def get_cube(self, cube_name: str):
        collection = Collection.query().filter(Collection.id == cube_name).first()

        if collection is None or not collection.is_cube:
            return 'Cube "{}" not found.'.format(cube_name), 404

        temporal = db.session.query(
            func.min(CollectionItem.composite_start).cast(sqlalchemy.String),
            func.max(CollectionItem.composite_end).cast(sqlalchemy.String)
        ).filter(CollectionItem.collection_id == collection.id).first()

        bands = Band.query().filter(Band.collection_id == cube_name).all()

        if temporal is None:
            temporal = []

        dump_collection = Serializer.serialize(collection)
        dump_collection['temporal'] = temporal
        dump_collection['bands'] = [Serializer.serialize(b) for b in bands]

        return dump_collection, 200

    def list_tiles_cube(self, cube_name: str):
        cube = Collection.query().filter(Collection.id == cube_name).first()

        if cube is None or not cube.is_cube:
            return 'Cube "{}" not found.'.format(cube_name), 404

        features = db.session.query(
                func.ST_AsGeoJSON(func.ST_SetSRID(Tile.geom_wgs84, 4326), 6, 3).cast(sqlalchemy.JSON)
            ).distinct(Tile.id).filter(
                CollectionItem.tile_id == Tile.id,
                CollectionItem.collection_id == cube_name,
                Tile.grs_schema_id == cube.grs_schema_id
            ).all()

        return [feature[0] for feature in features], 200

    def list_grs_schemas(self):
        """Retrieve a list of available Grid Schema on Brazil Data Cube database."""
        schemas = GrsSchema.query().all()

        return [Serializer.serialize(schema) for schema in schemas], 200

    def get_grs_schema(self, grs_id):
        """Retrieves a Grid Schema definition with tiles associated."""
        schema = GrsSchema.query().filter(GrsSchema.id == grs_id).first()

        if schema is None:
            return 'GRS {} not found.'.format(grs_id), 404

        tiles = db.session.query(
            Tile.id,
            func.ST_AsGeoJSON(func.ST_SetSRID(Tile.geom_wgs84, 4326), 6, 3).cast(sqlalchemy.JSON).label('geom_wgs84')
        ).filter(Tile.grs_schema_id == grs_id).all()

        dump_grs = Serializer.serialize(schema)
        dump_grs['tiles'] = [dict(id=t.id, geom_wgs84=t.geom_wgs84) for t in tiles]

        return dump_grs, 200

    def list_temporal_composition(self):
        """Retrieve a list of available Temporal Composition Schemas on Brazil Data Cube database."""
        schemas = TemporalCompositionSchema.query().all()

        return [Serializer.serialize(schema) for schema in schemas], 200

    def create_temporal_composition(self, temporal_schema, temporal_composite_t='', temporal_composite_unit=''):
        id = temporal_schema
        id += temporal_composite_t if temporal_composite_t != '' else 'null'
        id += temporal_composite_unit if temporal_composite_unit != '' else ''

        schema = TemporalCompositionSchema.query().filter(TemporalCompositionSchema.id == id).first()
        if schema:
            return 'Temporal Composition Schema {} already exists.'.format(id), 409

        TemporalCompositionSchema(
            id=id,
            temporal_schema=temporal_schema,
            temporal_composite_t=temporal_composite_t,
            temporal_composite_unit=temporal_composite_unit
        ).save()

        return 'Temporal Composition Schema created with successfully', 201
        
    def list_composite_functions(self):
        """Retrieve a list of available Composite Functions on Brazil Data Cube database."""
        schemas = CompositeFunctionSchema.query().all()

        return [Serializer.serialize(schema) for schema in schemas], 200

    def list_buckets(self):
        """Retrieve a list of available bucket in aws account."""
        buckets = self.services.list_repositories()

        return buckets, 200

    def list_raster_size(self):
        """Retrieve a list of available Raster Size Schemas on Brazil Data Cube database."""
        schemas = RasterSizeSchema.query().all()

        return [Serializer.serialize(schema) for schema in schemas], 200

    def list_merges(self, data_cube: str, tile_id: str, start: str, end: str):
        parts = get_cube_parts(data_cube)

        # Temp workaround to remove composite function from data cube name
        if len(parts) == 4:
            data_cube = '_'.join(parts[:-2])
        elif len(parts) == 3:
            data_cube = '_'.join(parts[:-1])

        items = self.services.get_merges(data_cube, tile_id, start, end)

        result = validate_merges(items)

        return result, 200

    def list_cube_items(self, data_cube: str, bbox: str = None, start: str = None,
                        end: str = None, page: int = 1, per_page: int = 10):
        cube = Collection.query().filter(Collection.id == data_cube).first()

        if cube is None:
            return 'Cube "{}" not found.'.format(data_cube), 404

        where = [
            CollectionItem.collection_id == data_cube,
            Tile.grs_schema_id == cube.grs_schema_id,
            Tile.id == CollectionItem.tile_id
        ]

        if start:
            where.append(CollectionItem.composite_start >= start)

        if end:
            where.append(CollectionItem.composite_end <= end)

        if bbox:
            xmin, ymin, xmax, ymax = [float(coord) for coord in bbox.split(',')]
            where.append(
                func.ST_Intersects(
                    func.ST_SetSRID(Tile.geom_wgs84, 4326), func.ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)
                )
            )

        paginator = db.session.query(CollectionItem).filter(
            *where
        ).order_by(CollectionItem.item_date.desc()).paginate(int(page), int(per_page), error_out=False)

        result = []

        for item in paginator.items:
            assets = db.session.query(
                db_functions.concat('s3://', Asset.url).label('url'),
                Band.name.label('band')
            ).filter(Asset.collection_item_id == item.id, Asset.band_id == Band.id).all()

            obj = Serializer.serialize(item)
            obj['quicklook'] = item.quicklook
            obj['assets'] = [dict(url=asset.url, band=asset.band) for asset in assets]

            result.append(obj)

        return dict(
            items=result,
            page=page,
            per_page=page,
            total_items=paginator.total,
            total_pages=paginator.pages
        ), 200

    def create_bucket(self, name, requester_pay):
        service = self.services

        status = service.create_bucket(name, requester_pay)
        if not status:
            return 'Bucket {} already exists.'.format(name), 409

        return 'Bucket created with successfully', 201