#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import json
import rasterio
import sqlalchemy
from copy import deepcopy
from datetime import datetime
from geoalchemy2 import func
from geoalchemy2.shape import from_shape
from shapely.geometry import Polygon
from werkzeug.exceptions import BadRequest, NotFound, Conflict

from bdc_catalog.models.base_sql import BaseModel, db
from bdc_catalog.models import (Collection, Band, BandSRC, GridRefSys, Tile, 
                                CompositeFunction, MimeType, ResolutionUnit, 
                                Quicklook, Item)

from .constants import (CLEAR_OBSERVATION_ATTRIBUTES, PROVENANCE_ATTRIBUTES, 
                        TOTAL_OBSERVATION_ATTRIBUTES, CLEAR_OBSERVATION_NAME, 
                        TOTAL_OBSERVATION_NAME, PROVENANCE_NAME, SRID_BDC_GRID, 
                        CENTER_WAVELENGTH, FULL_WIDTH_HALF_MAX, REVISIT_BY_SATELLITE,
                        COG_MIME_TYPE)
from .forms import CollectionForm
from .utils.serializer import Serializer
from .utils.processing import get_or_create_model, get_date, get_cube_name, get_cube_parts, decode_periods, generate_hash_md5, format_version
from .utils.image import validate_merges
from .maestro import orchestrate, prepare_merge, \
    merge_warped, solo, blend, posblend, publish
from .services import CubeServices

class CubeController:

    def __init__(self, url_stac=None, bucket=None):
        self.score = {}

        self.services = CubeServices(url_stac, bucket)


    @staticmethod
    def get_cube_or_404(cube_id=None, cube_full_name: str = '-'):
        """Try to retrieve a data cube on database and raise 404 when not found."""
        if cube_id:
            return Collection.query().filter(Collection.id == cube_id).first_or_404()
        else:
            cube_fragments = cube_full_name.split('-')
            cube_name = '-'.join(cube_fragments[:-1])
            cube_version = cube_fragments[-1]
            return Collection.query().filter(
                Collection.name == cube_name,
                Collection.version == cube_version
            ).first_or_404()


    @staticmethod
    def get_grid_by_name(grid_name: str):
        """Try to get a grid, otherwise raises 404."""
        grid = GridRefSys.query().filter(GridRefSys.name == grid_name).first()
        if not grid:
            raise NotFound(f'Grid {grid_name} not found.')
        return grid


    @staticmethod
    def get_mime_type(name: str):
        """Try to get a mime_type, otherwise raises 404."""
        mime_type = MimeType.query().filter(MimeType.name == name).first()
        if not mime_type:
            raise NotFound(f'Mime Type {name} not found.')
        return mime_type


    @staticmethod
    def get_resolution_unit(symbol: str):
        """Try to get a resolution, otherwise raises 404."""
        resolution_unit = ResolutionUnit.query().filter(ResolutionUnit.symbol == symbol).first()
        if not resolution_unit:
            raise NotFound(f'Resolution Unit {symbol} not found.')
        return resolution_unit


    @staticmethod
    def _validate_band_metadata(metadata: dict, band_map: dict) -> dict:
        bands = []

        for band in metadata['expression']['bands']:
            bands.append(band_map[band].id)

        metadata['expression']['bands'] = bands

        return metadata

    
    @classmethod
    def get_or_create_band(cls, cube, name, common_name, min_value, max_value,
                           nodata, data_type, resolution_x, resolution_y, scale,
                           resolution_unit_id, description=None) -> Band:
        """Get or try to create a data cube band on database.
        Notes:
            When band not found, it adds a new Band object to the SQLAlchemy db.session.
            You may need to commit the session after execution.
        Returns:
            A SQLAlchemy band object.
        """
        where = dict(
            collection_id=cube,
            name=name
        )

        defaults = dict(
            min_value=min_value,
            max_value=max_value,
            nodata=nodata,
            data_type=data_type,
            description=description,
            scale=scale,
            common_name=common_name,
            resolution_x=resolution_x,
            resolution_y=resolution_y,
            resolution_unit_id=resolution_unit_id
        )

        band, _ = get_or_create_model(Band, defaults=defaults, **where)

        return band


    @classmethod
    def _create_cube_definition(cls, cube_id: str, params: dict) -> dict:
        """Create a data cube definition.
        Basically, the definition consists in `Collection` and `Band` attributes.
        Note:
            It does not try to create when data cube already exists.
        Args:
            cube_id - Data cube
            params - Dict of required values to create data cube. See @validators.py
        Returns:
            A serialized data cube information.
        """
        cube_parts = get_cube_parts(cube_id)

        function = cube_parts.composite_function

        cube_id = cube_parts.datacube

        cube = Collection.query().filter(Collection.name == cube_id, Collection.version == params['version']).first()

        grs = GridRefSys.query().filter(GridRefSys.name == params['grs']).first()

        if grs is None:
            abort(404, f'Grid {params["grs"]} not found.')

        cube_function = CompositeFunction.query().filter(CompositeFunction.alias == function).first()

        if cube_function is None:
            abort(404, f'Function {function} not found.')

        data = dict(name='Meter', symbol='m')
        resolution_meter, _ = get_or_create_model(ResolutionUnit, defaults=data, symbol='m')

        mime_type, _ = get_or_create_model(MimeType, defaults=dict(name=COG_MIME_TYPE), name=COG_MIME_TYPE)

        if cube is None:
            cube = Collection(
                name=cube_id,
                title=params['title'],
                temporal_composition_schema=params['temporal_composition'] if function != 'IDT' else None,
                composite_function_id=cube_function.id,
                grs=grs,
                _metadata=params['metadata'],
                description=params['description'],
                collection_type='cube',
                is_public=params.get('public', True),
                version=params['version']
            )

            cube.save(commit=False)

            bands = []

            default_bands = (CLEAR_OBSERVATION_NAME.lower(), TOTAL_OBSERVATION_NAME.lower(), PROVENANCE_NAME.lower())

            band_map = dict()

            for band in params['bands']:
                name = band['name'].strip()

                if name in default_bands:
                    continue

                is_not_cloud = params['quality_band'] != band['name']

                if band['name'] == params['quality_band']:
                    data_type = 'uint8'
                else:
                    data_type = band['data_type']

                band_model = Band(
                    name=name,
                    common_name=band['common_name'],
                    collection=cube,
                    min_value=0,
                    max_value=10000 if is_not_cloud else 4,
                    nodata=-9999 if is_not_cloud else 255,
                    scale=0.0001 if is_not_cloud else 1,
                    data_type=data_type,
                    resolution_x=params['resolution'],
                    resolution_y=params['resolution'],
                    resolution_unit_id=resolution_meter.id,
                    description='',
                    mime_type_id=mime_type.id
                )

                if band.get('metadata'):
                    band_model._metadata = cls._validate_band_metadata(deepcopy(band['metadata']), band_map)

                band_model.save(commit=False)
                bands.append(band_model)

                band_map[name] = band_model

                if band_model._metadata:
                    for _band_origin_id in band_model._metadata['expression']['bands']:
                        band_provenance = BandSRC(band_src_id=_band_origin_id, band_id=band_model.id)
                        band_provenance.save(commit=False)

            quicklook = Quicklook(red=band_map[params['bands_quicklook'][0]].id,
                                  green=band_map[params['bands_quicklook'][1]].id,
                                  blue=band_map[params['bands_quicklook'][2]].id,
                                  collection=cube)

            quicklook.save(commit=False)

        # Create default Cube Bands
        if function != 'IDT':
            _ = cls.get_or_create_band(cube.id, **CLEAR_OBSERVATION_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                       resolution_x=params['resolution'], resolution_y=params['resolution'])
            _ = cls.get_or_create_band(cube.id, **TOTAL_OBSERVATION_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                       resolution_x=params['resolution'], resolution_y=params['resolution'])

            if function == 'STK':
                _ = cls.get_or_create_band(cube.id, **PROVENANCE_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                           resolution_x=params['resolution'], resolution_y=params['resolution'])

        if params.get('is_combined') and function != 'MED':
            _ = cls.get_or_create_band(cube.id, **DATASOURCE_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                       resolution_x=params['resolution'], resolution_y=params['resolution'])

        return CollectionForm().dump(cube)

    @classmethod
    def create(cls, params):
        """Create a data cube definition.
        Note:
            If you provide a data cube with composite function like MED, STK, it creates
            the cube metadata Identity and the given function name.
        Returns:
             Tuple with serialized cube and HTTP Status code, respectively.
        """
        cube_name = '{}_{}'.format(
            params['datacube'],
            int(params['resolution'])
        )

        params['bands'].extend(params['indexes'])

        with db.session.begin_nested():
            # Create data cube Identity
            cube = cls._create_cube_definition(cube_name, params)

            cube_serialized = [cube]

            if params['composite_function'] != 'IDT':
                step = params['temporal_composition']['step']
                unit = params['temporal_composition']['unit'][0].upper()
                temporal_str = f'{step}{unit}'

                cube_name_composite = f'{cube_name}_{temporal_str}_{params["composite_function"]}'

                # Create data cube with temporal composition
                cube_composite = cls._create_cube_definition(cube_name_composite, params)
                cube_serialized.append(cube_composite)

        db.session.commit()
        
        return cube_serialized, 201

    
    @classmethod
    def update(cls, cube_id: int, params):
        """Update data cube definition.
        Returns:
             Tuple with serialized cube and HTTP Status code, respectively.
        """
        with db.session.begin_nested():
            cube = cls.get_cube_or_404(cube_id=cube_id)

            cube.title = params['title']
            cube._metadata=params['metadata']
            cube.description=params['description']
            cube.is_public=params['public']
            
        db.session.commit()

        return {'message': 'Updated cube!'}, 200


    def get_cube_status(self, cube_name):
        cube = self.get_cube_or_404(cube_full_name=cube_name)
        datacube = cube.name

        # split and format datacube NAME
        parts_cube_name = get_cube_parts(datacube)
        irregular_datacube = '_'.join(parts_cube_name[:2])
        is_irregular = len(parts_cube_name) > 2
        datacube = '_'.join(get_cube_parts(datacube)[:3]) if is_irregular else irregular_datacube

        # STATUS
        acts_datacube = []
        not_done_datacube = 0
        error_datacube = 0
        if is_irregular:
            acts_datacube = self.services.get_activities_by_datacube(datacube)
            not_done_datacube = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', acts_datacube)))
            error_datacube = len(list(filter(lambda i: i['mystatus'] == 'ERROR', acts_datacube)))
        acts_irregular = self.services.get_activities_by_datacube(irregular_datacube)
        not_done_irregular = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', acts_irregular)))
        error_irregular = len(list(filter(lambda i: i['mystatus'] == 'ERROR', acts_irregular)))

        activities = acts_irregular + acts_datacube
        errors = error_irregular + error_datacube
        not_done = not_done_irregular + not_done_datacube
        if (not_done + errors):
            return dict(
                finished = False,
                done = len(activities) - (not_done + errors),
                not_done = not_done,
                error = errors
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

        quantity_coll_items = Item.query().filter(
            Item.collection_id == cube.id
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
        response = {}
        if params.get('process_id'):
            response = self.services.get_process_by_id(params['process_id'])

        if 'Items' not in response or len(response['Items']) == 0:
            raise NotFound('Process ID not found!')

        # get process infos by dynameDB
        process_info = response['Items'][0]
        functions = json.loads(process_info['functions'])
        indexes = json.loads(process_info['indexes'])
        quality_band = process_info['quality_band']
        datacube_name = process_info['datacube'].split('_')[0]
        version = int(process_info['datacube'].split('_')[1])
        datacube_suffix = process_info['datacube_suffix']
        satellite = process_info['platform_code']

        # get one function if != IDT (IDENTITY)
        datacube = f'{datacube_name}_{datacube_suffix}'
        base_function = [func for func in functions if func != 'IDT'][0]
        cube_full_name = get_cube_name(datacube, base_function)
        tiles = params['tiles']
        start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').strftime('%Y-%m-%d')
        end_date = datetime.strptime(params['end_date'], '%Y-%m-%d').strftime('%Y-%m-%d') \
            if params.get('end_date') else datetime.now().strftime('%Y-%m-%d')

        # verify cube info
        cube_infos = Collection.query().filter(
            Collection.name == cube_full_name,
            Collection.version == version
        ).first()
        cube_infos_irregular = Collection.query().filter(
            Collection.name == get_cube_name(datacube),
            Collection.version == version
        ).first()
        if not cube_infos or not cube_infos_irregular:
            return 'Cube not found!', 404

        # get bands list
        bands = Band.query().filter(
            Band.collection_id == cube_infos_irregular.id
        ).all()
        bands_list = []
        indexes_list = []
        for band in bands:
            if band.name.upper() not in [i.upper() for i in indexes]:
                bands_list.append(band.name)
            else:
                indexes_available = {
                    'NDVI': ['NIR', 'RED'],
                    'EVI': ['NIR', 'RED', 'BLUE']
                }
                if not indexes_available.get(band.name.upper()):
                    return 'Index not available', 400
                
                index = dict(
                    name=band.name,
                    bands=[
                        dict(
                            name=b.name,
                            common_name=b.common_name
                        ) for b in bands \
                            if b.common_name.upper() in indexes_available[band.name.upper()]
                    ]
                )
                if len(index['bands']) != len(indexes_available[band.name.upper()]):
                    return 'bands: {}, are needed to create the {} index'.format(
                        ','.join(indexes_available[band.name.upper()]), band.name), 400
                indexes_list.append(index)

        # get quicklook bands
        bands_ql = Quicklook.query().filter(
            Quicklook.collection_id == cube_infos_irregular.id
        ).first()
        bands_ql_list = [
            list(filter(lambda b: b.id == bands_ql.red, bands))[0].name, 
            list(filter(lambda b: b.id == bands_ql.green, bands))[0].name, 
            list(filter(lambda b: b.id == bands_ql.blue, bands))[0].name
        ]

        # items => old mosaic
        # orchestrate
        self.score['items'] = orchestrate(cube_infos_irregular, tiles, start_date, end_date, functions)

        # prepare merge
        crs = cube_infos.grs.crs
        formatted_version = format_version(version)
        prepare_merge(self, datacube, params['collections'], satellite, bands_list,
            indexes_list, bands_ql_list, float(bands[0].resolution_x), float(bands[0].resolution_y), 
            int(bands[0].nodata), crs, quality_band, functions, formatted_version, 
            params.get('force', False))

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

        # dispatch POS BLEND
        elif params['action'] == 'posblend':
            posblend(self, params)

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
        tile_srs_p4 = "+proj=longlat +ellps=GRS80 +datum=GRS80 +no_defs"
        if projection == 'aea':
            tile_srs_p4 = "+proj=aea +lat_0=-12 +lon_0={} +lat_1=-2 +lat_2=-22 +x_0=5000000 +y_0=10000000 +ellps=GRS80 +units=m +no_defs".format(meridian)
        elif projection == 'sinu':
            tile_srs_p4 = "+proj=sinu +lon_0={} +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs".format(meridian)

        # Number of tiles and base tile
        num_tiles_x = int(360./degreesx)
        num_tiles_y = int(180./degreesy)
        h_base = num_tiles_x/2
        v_base = num_tiles_y/2

        # Tile size in meters (dx,dy) at center of system (argsmeridian,0.)
        src_crs = '+proj=longlat +ellps=GRS80 +datum=GRS80 +no_defs'
        dst_crs = tile_srs_p4
        xs = [(meridian - degreesx/2), (meridian + degreesx/2), meridian, meridian, 0.]
        ys = [0., 0., -degreesy/2, degreesy/2, 0.]
        out = rasterio.warp.transform(src_crs, dst_crs, xs, ys, zs=None)
        x1 = out[0][0]
        x2 = out[0][1]
        y1 = out[1][2]
        y2 = out[1][3]
        dx = x2-x1
        dy = y2-y1

        # Coordinates of WRS center (0.,0.) - top left coordinate of (h_base,v_base)
        xCenter =  out[0][4]
        yCenter =  out[1][4]
        # Border coordinates of WRS grid
        xMin = xCenter - dx*h_base
        yMax = yCenter + dy*v_base

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

        tiles = []
        features = []
        dst_crs = '+proj=longlat +ellps=GRS80 +datum=GRS80 +no_defs'
        src_crs = tile_srs_p4
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

                polygon = from_shape(
                    Polygon(
                        [
                            (x1, y2),
                            (x2, y2),
                            (x2, y1),
                            (x1, y1),
                            (x1, y2)
                        ]
                    ), 
                    srid=SRID_BDC_GRID
                )

                # Insert tile
                tile_name = '{0:03d}{1:03d}'.format(ix, iy)
                tiles.append(dict(
                    name=tile_name
                ))
                features.append(dict(
                    tile=tile_name,
                    geom=polygon
                ))
        
        with db.session.begin_nested():
            grs = GridRefSys.create_geometry_table(table_name=name.lower(), features=features, srid=SRID_BDC_GRID)
            grs.description = description
            db.session.add(grs)

            [db.session.add(Tile(**tile, grs=grs)) for tile in tiles]
        db.session.commit()        

        return 'Grid {} created with successfully'.format(name), 201

    def list_grs_schemas(self):
        """Retrieve a list of available Grid Schema on Brazil Data Cube database."""
        schemas = GridRefSys.query().all()

        return [dict(**Serializer.serialize(schema), crs=schema.crs) for schema in schemas], 200

    def get_grs_schema(self, grs_id):
        """Retrieves a Grid Schema definition with tiles associated."""
        schema = GridRefSys.query().filter(GridRefSys.id == grs_id).first()

        if schema is None:
            return 'GRS {} not found.'.format(grs_id), 404

        geom_table = schema.geom_table
        tiles = db.session.query(
            geom_table.c.tile,
            func.ST_AsGeoJSON(func.ST_Transform(geom_table.c.geom, 4326), 6, 3).cast(sqlalchemy.JSON).label('geom_wgs84')
        ).all()

        dump_grs = Serializer.serialize(schema)
        dump_grs['tiles'] = [dict(id=t.tile, geom_wgs84=t.geom_wgs84) for t in tiles]

        return dump_grs, 200
    
    def list_cubes(self):
        """Retrieve the list of data cubes from Brazil Data Cube database."""
        cubes = Collection.query().filter(Collection.collection_type == 'cube').all()

        serializer = CollectionForm()

        list_cubes = []
        for cube in cubes:
            cube_dict = serializer.dump(cube)
            not_done = 0
            sum_acts = 0
            error = 0
            if cube.composite_function.alias != 'IDT':
                activities = self.services.get_activities_by_datacube(cube.name)
                not_done = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', activities)))
                error = len(list(filter(lambda i: i['mystatus'] == 'ERROR', activities)))
                sum_acts += len(activities)

            parts = get_cube_parts(cube.name)
            data_cube_identity = '_'.join(parts[:2])
            activities = self.services.get_activities_by_datacube(data_cube_identity)
            not_done_identity = len(list(filter(lambda i: i['mystatus'] == 'NOTDONE', activities)))
            error_identity = len(list(filter(lambda i: i['mystatus'] == 'ERROR', activities)))
            sum_acts += len(activities)
            
            cube_dict['status'] = 'Pending'
            if sum_acts > 0:
                sum_not_done = not_done + not_done_identity
                sum_errors = error + error_identity
                cube_dict['status'] = 'Error' if sum_errors > 0 else 'Finished' \
                    if (sum_not_done + sum_errors) == 0 else 'Pending'
            list_cubes.append(cube_dict)

        return list_cubes, 200

    def get_cube(self, cube_id: int):
        cube = self.get_cube_or_404(cube_id)

        dump_cube = Serializer.serialize(cube)
        dump_cube['bands'] = [Serializer.serialize(b) for b in cube.bands]
        dump_cube['quicklook'] = [
            list(filter(lambda b: b.id == cube.quicklook[0].red, cube.bands))[0].name,
            list(filter(lambda b: b.id == cube.quicklook[0].green, cube.bands))[0].name,
            list(filter(lambda b: b.id == cube.quicklook[0].blue, cube.bands))[0].name
        ]
        dump_cube['extent'] = None
        dump_cube['grid'] = cube.grs.name
        dump_cube['composite_function'] = cube.composite_function.name

        return dump_cube, 200

    def list_tiles_cube(self, cube_id: int):
        cube = self.get_cube_or_404(cube_id)

        geom_table = cube.grs.geom_table
        features = db.session.query(
            func.ST_AsGeoJSON(func.ST_Transform(geom_table.c.geom, 4326), 6, 3).cast(sqlalchemy.JSON)
        ).all()

        return [feature[0] for feature in features], 200

    def list_composite_functions(self):
        """Retrieve a list of available Composite Functions on Brazil Data Cube database."""
        schemas = CompositeFunction.query().all()

        return [Serializer.serialize(schema) for schema in schemas], 200

    def create_bucket(self, name, requester_pay):
        service = self.services

        status = service.create_bucket(name, requester_pay)
        if not status:
            return 'Bucket {} already exists.'.format(name), 409

        return 'Bucket created with successfully', 201

    def list_buckets(self):
        """Retrieve a list of available bucket in aws account."""
        buckets = self.services.list_repositories()

        return buckets, 200

    def list_merges(self, cube_id: str, tile_id: str, start: str, end: str):
        cube = self.get_cube_or_404(cube_id)

        parts = get_cube_parts(cube.name)

        # Temp workaround to remove composite function from data cube name
        if len(parts) == 4:
            data_cube = '_'.join(parts[:-2])
        elif len(parts) == 3:
            data_cube = '_'.join(parts[:-1])

        items = self.services.get_merges(data_cube, tile_id, start, end)

        result = validate_merges(items)

        return result, 200

    def list_cube_items(self, cube_id: str, bbox: str = None, start: str = None,
                        end: str = None, tiles: str = None, page: int = 1, per_page: int = 10):
        cube = self.get_cube_or_404(cube_id)

        where = [
            Item.collection_id == cube_id,
            Tile.id == Item.tile_id
        ]

        # temporal filter
        if start:
            where.append(Item.start_date >= start)
        if end:
            where.append(Item.end_date <= end)

        # tile(string) filter
        if tiles:
            tiles = tiles.split(',') if isinstance(tiles, str) else tiles
            where.append(
                Tile.name.in_(tiles)
            )

        # spatial filter
        if bbox:
            xmin, ymin, xmax, ymax = [float(coord) for coord in bbox.split(',')]
            where.append(
                func.ST_Intersects(
                    func.ST_SetSRID(Item.geom, 4326), func.ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)
                )
            )

        paginator = db.session.query(Item).filter(
            *where
        ).order_by(Item.start_date.desc()).paginate(int(page), int(per_page), error_out=False)

        result = []
        for item in paginator.items:
            item.geom = None
            item.min_convex_hull = None
            item.tile_id = item.tile.name
            obj = Serializer.serialize(item)
            if item.assets.get('thumbnail'):
                obj['quicklook'] = item.assets['thumbnail']['href']
            del obj['assets']
                    
            result.append(obj)

        return dict(
            items=result,
            page=page,
            per_page=page,
            total_items=paginator.total,
            total_pages=paginator.pages
        ), 200

    def list_timeline(self, schema: str, step: int, start: str = None, end: str = None):
        """Generate data cube periods using temporal composition schema.

        Args:
            schema: Temporal Schema (M, A)
            step: Temporal Step
            start: Start date offset. Default is '2016-01-01'.
            end: End data offset. Default is '2019-12-31'

        Returns:
            List of periods between start/last date
        """
        start_date = start or '2016-01-01'
        last_date = end or '2019-12-31'

        total_periods = decode_periods(schema, start_date, last_date, int(step))

        periods = set()

        for period_array in total_periods.values():
            for period in period_array:
                date = period.split('_')[0]

                periods.add(date)

        return sorted(list(periods)), 200

    def list_cube_items_tiles(self, cube_id: int):
        """Retrieve the tiles which data cube is generated."""
        tiles = db.session.query(Item)\
            .filter(Item.collection_id == cube_id)\
            .all()

        return set([t.tile.name for t in tiles]), 200

    def get_cube_meta(self, cube_id: str):
        """Retrieve the data cube metadata used to build a data cube items.

        The metadata includes:
        - STAC provider url
        - Collection used to generate.

        Note:
            When there is no data cube item generated yet, raises BadRequest.
        """
        cube = self.get_cube_or_404(cube_id)

        identity_cube = '_'.join(cube.name.split('_')[:2])

        item = self.services.get_cube_meta(identity_cube)

        if item is None or len(item['Items']) == 0:
            raise BadRequest('There is no data cube activity')

        activity = json.loads(item['Items'][0]['activity'])

        return dict(
            url_stac=activity['url_stac'],
            collections=','.join(activity['datasets']),
            bucket=activity['bucket_name'],
            satellite=activity['satellite'],
        ), 200

    # def estimate_cost(self, satellite, resolution, grid, start_date, last_date,
    #                     quantity_bands, quantity_tiles, t_schema, t_step, quantity_indexes):
    #     """ compute STORAGE :: """
    #     tile = db.session() \
    #         .query(
    #             Tile, func.ST_Xmin(Tile.geom), func.ST_Xmax(Tile.geom),
    #             func.ST_Ymin(Tile.geom), func.ST_Ymax(Tile.geom)
    #         ).filter(
    #             Tile.grs_schema_id == grid
    #         ).first()
    #     if not tile:
    #         'GRS not found!', 404
    #     raster_size_x = int(round((tile[2]-tile[1])/int(resolution),0))
    #     raster_size_y = int(round((tile[4]-tile[3])/int(resolution),0))
    #     size_tile = (((raster_size_x * raster_size_y) * 2) / 1024) / 1024

    #     periods = decode_periods(t_schema, start_date, last_date, int(t_step))
    #     len_periods = len(periods.keys()) if t_schema == 'M' else sum([len(p) for p in periods.values()])

    #     cube_size = size_tile * (quantity_bands + quantity_indexes) * quantity_tiles * len_periods
    #     # with COG and compress = +50%
    #     cube_size = (cube_size * 1.5) / 1024 # in GB
    #     cubes_size = cube_size * 2
    #     price_cubes_storage = cubes_size * 0.024 # in U$

    #     # irregular cube
    #     revisit_by_sat = REVISIT_BY_SATELLITE.get(satellite)
    #     start_date = datetime.strptime(start_date, '%Y-%m-%d')
    #     last_date = datetime.strptime(last_date, '%Y-%m-%d')
    #     scenes = ((last_date - start_date).days) / revisit_by_sat
    #     irregular_cube_size = (size_tile * (quantity_bands + quantity_indexes) * quantity_tiles * scenes) / 1024 # in GB
    #     price_irregular_cube_storage = irregular_cube_size * 0.024 # in U$

    #     """ compute PROCESSING :: """
    #     quantity_merges = quantity_bands * quantity_tiles * scenes
    #     quantity_blends = quantity_bands * quantity_tiles * len_periods
    #     # quantity_indexes * 2 => Cubos (regulares e Irregulares)
    #     quantity_posblends = (quantity_indexes * 2) * quantity_tiles * len_periods
    #     quantity_publish = quantity_tiles * len_periods

    #     # cost
    #     # merge (100 req (2560MB, 70000ms) => 0.23)
    #     cost_merges = (quantity_merges / 100) * 0.29
    #     # blend (100 req (2560MB, 200000ms) => 0.83)
    #     cost_blends = (quantity_blends / 100) * 0.83
    #     # posblend (100 req (2560MB, 360000ms) => 1.50)
    #     cost_posblends = (quantity_posblends / 100) * 1.50
    #     # publish (100 req (256MB, 60000ms) => 0.03)
    #     cost_publish = (quantity_publish / 100) * 0.03

    #     return dict(
    #         storage=dict(
    #             size_cubes=float(cubes_size),
    #             price_cubes=float(price_cubes_storage),
    #             size_irregular_cube=float(irregular_cube_size),
    #             price_irregular_cube=float(price_irregular_cube_storage)
    #         ),
    #         build=dict(
    #             quantity_merges=int(quantity_merges),
    #             quantity_blends=int(quantity_blends),
    #             quantity_posblends=int(quantity_posblends),
    #             quantity_publish=int(quantity_publish),
    #             collection_items_irregular=int((quantity_tiles * scenes)),
    #             collection_items=int((quantity_tiles * len_periods) * 2),
    #             price_merges=float(cost_merges),
    #             price_blends=float(cost_blends),
    #             price_posblends=float(cost_posblends),
    #             price_publish=float(cost_publish)
    #         )
    #     ), 200
