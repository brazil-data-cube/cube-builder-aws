#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""Controller module."""

import json
from copy import deepcopy
from datetime import datetime
from typing import Tuple, Union

import rasterio
import sqlalchemy
from bdc_catalog.models import (Band, BandSRC, Collection, CompositeFunction,
                                GridRefSys, Item, MimeType, Quicklook,
                                ResolutionUnit, SpatialRefSys, Tile)
from bdc_catalog.models.base_sql import BaseModel, db
from geoalchemy2 import func
from geoalchemy2.shape import from_shape
from rasterio.crs import CRS
from rasterio.warp import transform
from shapely.geometry import Polygon
from werkzeug.exceptions import BadRequest, Conflict, NotFound

from .config import ITEM_PREFIX
from .constants import (CENTER_WAVELENGTH, CLEAR_OBSERVATION_ATTRIBUTES,
                        CLEAR_OBSERVATION_NAME, COG_MIME_TYPE,
                        DATASOURCE_ATTRIBUTES, FULL_WIDTH_HALF_MAX,
                        PROVENANCE_ATTRIBUTES, PROVENANCE_NAME,
                        REVISIT_BY_SATELLITE, SRID_ALBERS_EQUAL_AREA,
                        SRID_BDC_GRID, TOTAL_OBSERVATION_ATTRIBUTES,
                        TOTAL_OBSERVATION_NAME)
from .forms import CollectionForm
from .maestro import (blend, merge_warped, orchestrate, posblend,
                      prepare_merge, publish, solo)
from .services import CubeServices
from .utils.image import validate_merges
from .utils.processing import (format_version, generate_hash_md5,
                               get_cube_name, get_cube_parts, get_date,
                               get_or_create_model)
from .utils.serializer import DecimalEncoder, Serializer
from .utils.timeline import Timeline


class CubeController:
    """Controller class."""

    def __init__(self, url_stac=None, bucket=None):
        self.score = {}

        self.services = CubeServices(url_stac, bucket)


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


    def _create_cube_definition(self, cube_id: str, params: dict) -> dict:
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
            raise NotFound(f'Grid {params["grs"]} not found.')

        cube_function = CompositeFunction.query().filter(CompositeFunction.alias == function).first()

        if cube_function is None:
            raise NotFound(f'Function {function} not found.')

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
                    band_model._metadata = self._validate_band_metadata(deepcopy(band['metadata']), band_map)

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
            _ = self.get_or_create_band(cube.id, **CLEAR_OBSERVATION_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                       resolution_x=params['resolution'], resolution_y=params['resolution'])
            _ = self.get_or_create_band(cube.id, **TOTAL_OBSERVATION_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                       resolution_x=params['resolution'], resolution_y=params['resolution'])

            if function == 'STK':
                _ = self.get_or_create_band(cube.id, **PROVENANCE_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                           resolution_x=params['resolution'], resolution_y=params['resolution'])

        if params.get('is_combined') and function != 'MED':
            _ = self.get_or_create_band(cube.id, **DATASOURCE_ATTRIBUTES, resolution_unit_id=resolution_meter.id,
                                       resolution_x=params['resolution'], resolution_y=params['resolution'])

        return CollectionForm().dump(cube)

    def create(self, params):
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

        datacube = dict()
        irregular_datacube = dict()

        with db.session.begin_nested():
            # Create data cube Identity
            cube = self._create_cube_definition(cube_name, params)
            irregular_datacube = cube

            cube_serialized = [cube]

            if params['composite_function'] != 'IDT':
                step = params['temporal_composition']['step']
                unit = params['temporal_composition']['unit'][0].upper()
                temporal_str = f'{step}{unit}'

                cube_name_composite = f'{cube_name}_{temporal_str}_{params["composite_function"]}'

                # Create data cube with temporal composition
                cube_composite = self._create_cube_definition(cube_name_composite, params)
                cube_serialized.append(cube_composite)
                datacube = cube_composite

        db.session.commit()

        # set infos in process table (dynamoDB)
        # delete if exists
        response = self.services.get_process_by_datacube(datacube['id'])
        if 'Items' not in response or len(response['Items']) == 0:
            for item in response['Items']:
                self.services.remove_process_by_key(item['id'])

        # add new process
        cube_identify = f'{datacube["name"]}-{datacube["version"]}'
        self.services.put_process_table(
            key=cube_identify,
            datacube_id=datacube['id'],
            i_datacube_id=irregular_datacube['id'],
            infos=params
        )
        
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

        return dict(
            message='Updated cube!'
        ), 200


    def get_cube_status(self, cube_name):
        cube = self.get_cube_or_404(cube_full_name=cube_name)
        datacube = cube.name

        # split and format datacube NAME
        parts_cube_name = get_cube_parts(datacube)
        irregular_datacube = '_'.join(parts_cube_name[:2])
        is_regular = len(parts_cube_name) > 2

        # STATUS
        acts_datacube = []
        not_done_datacube = 0
        error_datacube = 0
        if is_regular:
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
        if len(acts):
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

        return dict(
            finished = False,
            done = 0,
            not_done = 0,
            error = 0
        ), 200

    def start_process(self, params):
        response = {}
        datacube_identify = f'{params["datacube"]}-{params["datacube_version"]}'
        response = self.services.get_process_by_id(datacube_identify)

        if 'Items' not in response or len(response['Items']) == 0:
            raise NotFound('Datacube not found in proccess table!')

        # get process infos by dynameDB
        process_info = response['Items'][0]
        process_params = json.dumps(process_info['infos'], cls=DecimalEncoder) 
        process_params = json.loads(process_params)

        indexes = process_params['indexes']
        quality_band = process_params['quality_band']
        functions = [process_params['composite_function'], 'IDT']
        satellite = process_params['metadata']['platform']['code']
        mask = process_params['parameters'].get('mask')
        if not mask:
            raise NotFound('Mask values not found in item allocated in processing table - dynamoDB')

        secondary_catalog = process_params.get('secondary_catalog')

        tiles = params['tiles']
        start_date = params['start_date'].strftime('%Y-%m-%d')
        end_date = params['end_date'].strftime('%Y-%m-%d') \
            if params.get('end_date') else datetime.now().strftime('%Y-%m-%d')

        # verify cube info
        cube_infos = Collection.query().filter(
            Collection.id == process_info['datacube_id']
        ).first()
        cube_infos_irregular = Collection.query().filter(
            Collection.id == process_info['irregular_datacube_id']
        ).first()
        if not cube_infos or not cube_infos_irregular:
            return 'Cube not found!', 404

        # get bands list
        bands = Band.query().filter(
            Band.collection_id == cube_infos_irregular.id
        ).all()

        bands_expressions = dict()

        bands_list = []
        bands_ids_list = {}
        for band in bands:
            if band.name.upper() not in [i['common_name'].upper() for i in indexes]:
                bands_list.append(band.name)
                bands_ids_list[band.id] = band.name
            elif band._metadata and band._metadata.get('expression') and band._metadata['expression'].get('value'):
                meta = deepcopy(band._metadata)
                meta['data_type'] = band.data_type
                bands_expressions[band.name] = meta

        # get quicklook bands
        bands_ql = Quicklook.query().filter(
            Quicklook.collection_id == cube_infos_irregular.id
        ).first()
        bands_ql_list = [
            list(filter(lambda b: b.id == bands_ql.red, bands))[0].name, 
            list(filter(lambda b: b.id == bands_ql.green, bands))[0].name, 
            list(filter(lambda b: b.id == bands_ql.blue, bands))[0].name
        ]

        # items => { 'tile_id': bbox, xmin, ..., periods: {'start_end': collection, ... } }
        # orchestrate
        shape = params.get('shape', None)
        temporal_schema = cube_infos.temporal_composition_schema
        self.score['items'] = orchestrate(cube_infos_irregular, temporal_schema, tiles, start_date, end_date, shape, item_prefix=ITEM_PREFIX)

        # prepare merge
        crs = cube_infos.grs.crs
        formatted_version = format_version(cube_infos.version)
        prepare_merge(self, cube_infos.name, cube_infos_irregular.name, params['collections'], satellite,
            bands_list, bands_ids_list, bands_ql_list, float(bands[0].resolution_x), 
            float(bands[0].resolution_y), int(bands[0].nodata), crs, quality_band, functions, formatted_version, 
            params.get('force', False), mask, secondary_catalog, bands_expressions=bands_expressions)

        return dict(
            message='Processing started with succesfully'
        ), 201

    @classmethod
    def create_grs_schema(cls, name, description, projection, meridian, degreesx, degreesy, bbox, srid=SRID_ALBERS_EQUAL_AREA):
        """Create a Brazil Data Cube Grid Schema."""
        bbox = bbox.split(',')
        bbox_obj = {
            "w": float(bbox[0]),
            "n": float(bbox[1]),
            "e": float(bbox[2]),
            "s": float(bbox[3])
        }
        tile_srs_p4 = "+proj=longlat +ellps=GRS80 +no_defs"
        if projection == 'aea':
            tile_srs_p4 = "+proj=aea +lat_0=-12 +lon_0={} +lat_1=-2 +lat_2=-22 +x_0=5000000 +y_0=10000000 +ellps=GRS80 +units=m +no_defs".format(meridian)
        elif projection == 'sinu':
            tile_srs_p4 = "+proj=sinu +lon_0={} +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs".format(meridian)

        # Number of tiles and base tile
        num_tiles_x = int(360. / degreesx)
        num_tiles_y = int(180. / degreesy)
        h_base = num_tiles_x / 2
        v_base = num_tiles_y / 2

        # Tile size in meters (dx,dy) at center of system (argsmeridian,0.)
        src_crs = '+proj=longlat +ellps=GRS80 +no_defs'
        dst_crs = tile_srs_p4
        xs = [(meridian - degreesx / 2), (meridian + degreesx / 2), meridian, meridian, 0.]
        ys = [0., 0., -degreesy / 2, degreesy / 2, 0.]
        out = transform(src_crs, dst_crs, xs, ys, zs=None)
        x1 = out[0][0]
        x2 = out[0][1]
        y1 = out[1][2]
        y2 = out[1][3]
        dx = x2 - x1
        dy = y2 - y1

        # Coordinates of WRS center (0.,0.) - top left coordinate of (h_base,v_base)
        x_center = out[0][4]
        y_center = out[1][4]
        # Border coordinates of WRS grid
        x_min = x_center - dx * h_base
        y_max = y_center + dy * v_base


        # Upper Left is (xl,yu) Bottom Right is (xr,yb)
        xs = [bbox_obj['w'], bbox_obj['e'], meridian, meridian]
        ys = [0., 0., bbox_obj['n'], bbox_obj['s']]
        out = transform(src_crs, dst_crs, xs, ys, zs=None)
        xl = out[0][0]
        xr = out[0][1]
        yu = out[1][2]
        yb = out[1][3]
        h_min = int((xl - x_min) / dx)
        h_max = int((xr - x_min) / dx)
        v_min = int((y_max - yu) / dy)
        v_max = int((y_max - yb) / dy)

        tiles = []
        features = []
        src_crs = tile_srs_p4

        for ix in range(h_min, h_max+1):
            x1 = x_min + ix*dx
            x2 = x1 + dx
            for iy in range(v_min, v_max+1):
                y1 = y_max - iy*dy
                y2 = y1 - dy
                # Evaluate the bounding box of tile in longlat
                xs = [x1, x2, x2, x1]
                ys = [y1, y1, y2, y2]
                out = transform(src_crs, dst_crs, xs, ys, zs=None)

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
                    srid=srid
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
            crs = CRS.from_proj4(tile_srs_p4)
            data = dict(
                auth_name='Albers Equal Area',
                auth_srid=srid,
                srid=srid,
                srtext=crs.to_wkt(),
                proj4text=tile_srs_p4
            )

            spatial_index, _ = get_or_create_model(SpatialRefSys, defaults=data, srid=srid)

            grs = GridRefSys.create_geometry_table(table_name=name, features=features, srid=srid)
            grs.description = description
            db.session.add(grs)

            [db.session.add(Tile(**tile, grs=grs)) for tile in tiles]
        db.session.commit()        

        return dict(
            message='Grid {} created with successfully'.format(name)
        ), 201

    @classmethod
    def list_grs_schemas(cls):
        """Retrieve a list of available Grid Schema on Brazil Data Cube database."""
        schemas = GridRefSys.query().all()

        return [dict(**Serializer.serialize(schema), crs=schema.crs) for schema in schemas], 200

    @classmethod
    def get_grs_schema(cls, grs_id):
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

    @classmethod
    def list_tiles_cube(cls, cube_id: int, only_ids=False):
        """Retrieve all tiles (as GeoJSON) that belongs to a data cube."""
        features = db.session.query(
            Item.tile_id, 
            Tile,
            func.ST_AsGeoJSON(Item.geom, 6, 3).cast(sqlalchemy.JSON).label('geom')
        ).distinct(Item.tile_id).filter(Item.collection_id == cube_id, Item.tile_id == Tile.id).all()

        return [feature.Tile.name if only_ids else feature.geom for feature in features], 200

    @classmethod
    def list_composite_functions(cls):
        """Retrieve a list of available Composite Functions on Brazil Data Cube database."""
        schemas = CompositeFunction.query().all()

        return [Serializer.serialize(schema) for schema in schemas], 200

    def create_bucket(self, name, requester_pay):
        service = self.services

        status = service.create_bucket(name, requester_pay)
        if not status:
            return dict(
                message='Bucket {} already exists.'.format(name)
            ), 409

        return dict(
            message='Bucket created with successfully'
        ), 201

    def list_buckets(self):
        """Retrieve a list of available bucket in aws account."""
        buckets = self.services.list_repositories()

        return buckets, 200

    def check_for_invalid_merges(self, cube_id: str, tile_id: str, start_date: str, end_date: str) -> Tuple[dict, int]:
        """List merge files used in data cube and check for invalid scenes.
        Args:
            datacube: Data cube name
            tile: Brazil Data Cube Tile identifier
            start_date: Activity start date (period)
            end_date: Activity End (period)
        Returns:
            List of Images used in period
        """
        cube = self.get_cube_or_404(cube_id)

        cube_name_parts = cube.name.split('_')
        cube_identity = '_'.join(cube_name_parts[:2])

        items = self.services.get_merges(cube_identity, tile_id, start_date[:10], end_date[:10])

        result = validate_merges(items)

        return result, 200

    def list_cube_items(self, cube_id: str, bbox: str = None, start: str = None,
                        end: str = None, tiles: str = None, page: int = 1, per_page: int = 10):
        """Retrieve all data cube items done."""
        cube = self.get_cube_or_404(cube_id=cube_id)

        where = [
            Item.collection_id == cube.id,
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
            obj = Serializer.serialize(item)
            obj['geom'] = None
            obj['min_convex_hull'] = None
            obj['tile_id'] = item.tile.name
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

    def generate_periods(self, schema: str, step: int, unit: str, cycle: dict = None,
                         intervals = None, start_date: str = None, last_date: str = None):
        """Generate data cube periods using temporal composition schema.
        Args:
            schema: Temporal Schema (continuous, cyclic)
            step: Temporal Step
            unit: Temporal Unit (day, month, year)
            start_date: Start date offset. Default is '2016-01-01'.
            last_date: End data offset. Default is '2019-12-31'
        Returns:
            List of periods between start/last date
        """
        start_date = datetime.strptime((start_date or '2016-01-01')[:10], '%Y-%m-%d').date()
        last_date = datetime.strptime((last_date or '2019-12-31')[:10], '%Y-%m-%d').date()

        periods = Timeline(schema, start_date, last_date, unit, int(step), cycle, intervals).mount()

        return dict(
            timeline=[[str(period[0]), str(period[1])] for period in periods]
        )

    def get_cube_meta(self, cube_id: str):
        """Retrieve the data cube metadata used to build a data cube items.

        The metadata includes:
        - STAC provider url
        - Collection used to generate.

        Note:
            When there is no data cube item generated yet, raises BadRequest.
        """
        cube = self.get_cube_or_404(int(cube_id))

        identity_cube = '_'.join(cube.name.split('_')[:2])

        item = self.services.get_cube_meta(identity_cube)

        if item is None or len(item['Items']) == 0:
            raise BadRequest('There is no data cube activity')

        activity = json.loads(item['Items'][0]['activity'])

        return dict(
            stac_url=activity['url_stac'],
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
