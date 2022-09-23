#
# This file is part of Cube Builder AWS.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Define a utility to validate merge images."""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Tuple

import rasterio

from ..logger import logger as logging

LANDSAT_BANDS = dict(
    int16=['band1', 'band2', 'band3', 'band4', 'band5', 'band6', 'band7', 'evi', 'ndvi'],
    uint16=['pixel_qa']
)


def validate(activity_row: dict):
    """Validate each merge result."""
    activity = json.loads(activity_row['activity'])

    activity_row['activity'] = activity

    errors = list()

    if 'ERROR' in activity_row['mystatus']:
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
            merge_date = activity['date']
            band = item['activity']['band']

            output.setdefault(merge_date, dict())
            output[merge_date].setdefault('collections', activity['datasets'])
            output[merge_date].setdefault('errors', list())
            output[merge_date].setdefault('bands', dict())

            output[merge_date]['errors'].extend(errors)

            merge_file = 's3://{}/{}'.format(activity['bucket_name'], activity['ARDfile']) if activity.get('ARDfile') else None

            output[merge_date]['bands'].setdefault(band, dict(merge=merge_file, scenes=set()))

            for link in activity['links']:
                output[merge_date]['bands'][band]['scenes'].add(link)

        return output
