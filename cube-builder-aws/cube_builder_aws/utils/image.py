#
# This file is part of Python Module for Cube Builder.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define a utility to validate merge images."""

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Tuple
import rasterio

LANDSAT_BANDS = dict(
    int16=['band1', 'band2', 'band3', 'band4', 'band5', 'band6', 'band7', 'evi', 'ndvi'],
    uint16=['pixel_qa']
)


def validate(activity_row: dict):
    """Validate each merge result."""
    activity = json.loads(activity_row['activity'])

    activity_row['activity'] = activity

    errors = list()

    if 'ERROR' in activity_row['mystatus'] or activity_row['id'] == 'mergeS2_100890982019-01-15swir2':
        band = activity['band']

        for link in activity['links']:
            link = link.replace('chronos.dpi.inpe.br:8089/datastore', 'www.dpi.inpe.br/newcatalog/tmp')

            try:
                with rasterio.open(link, 'r') as data_set:
                    logging.debug('File {} ok'.format(link))

                    if activity_row['data_cube'].startswith('LC8'):
                        data_type = data_set.meta.get('dtype')

                        band_dtype = LANDSAT_BANDS.get(data_type)

                        if band_dtype is None:
                            errors.append(
                                dict(
                                    message='Band {} mismatch with default Landsat 8'.format(band),
                                    band=band,
                                    file=link
                                )
                            )

                        file_name = Path(link).stem

                        band_name = file_name.split('_')[8]

                        if band_name not in band_dtype:
                            errors.append(dict(
                                message='Band {} should be {}'.format(band, data_type),
                                band=band,
                                file=link
                            ))

            except rasterio.RasterioIOError as e:
                errors.append(dict(message='File not found or invalid.', band=band, file=link))

    return activity_row, errors


def validate_merges(activities: Tuple[dict], num_threads: int = 4) -> dict:
    """Validate each merge retrieved from ``Activity.list_merge_files``.

    Args:
        activities: Activity merge images
        num_threads: Concurrent processes to validate
    """
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = executor.map(validate, activities)

        output = dict()

        for item, errors in futures:
            if item is None:
                continue

            activity = item['activity']
            band = item['activity']['band']

            output.setdefault(item['period_start'], dict())
            output[item['period_start']].setdefault('collections', activity['datasets'])
            output[item['period_start']].setdefault('errors', list())
            output[item['period_start']].setdefault('bands', dict())

            output[item['period_start']]['errors'].extend(errors)

            merge_file = 's3://{}/{}'.format(activity['bucket_name'], activity['ARDfile']) if activity.get('ARDfile') else None

            output[item['period_start']]['bands'].setdefault(band, dict(merge=merge_file, scenes=set()))

            for link in activity['links']:
                output[item['period_start']]['bands'][band]['scenes'].add(link)

        return output
