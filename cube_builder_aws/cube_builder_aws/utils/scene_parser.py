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

"""
Utility functions to parser scene_id.
"""
import re
from typing import Any, Dict


def sentinel_2(scene_id, args: Dict[str, Any] = dict()):
    pattern = (
        r"^S"
        r"(?P<sensor>\w{1})"
        r"(?P<satellite>[AB]{1})"
        r"_"
        r"MSI(?P<processingLevel>L[0-2][ABC])"
        r"_"
        r"(?P<acquisitionYear>[0-9]{4})"
        r"(?P<acquisitionMonth>[0-9]{2})"
        r"(?P<acquisitionDay>[0-9]{2})"
        r"T(?P<acquisitionHMS>[0-9]{6})"
        r"_"
        r"N(?P<baseline_number>[0-9]{4})"
        r"_"
        r"R(?P<relative_orbit>[0-9]{3})"
        r"_T"
        r"(?P<utm>[0-9]{2})"
        r"(?P<lat>\w{1})"
        r"(?P<sq>\w{2})"
        r"_"
        r"(?P<stopDateTime>[0-9]{8}T[0-9]{6})$"
    )
    meta: Dict[str, Any] = re.match(pattern, scene_id, re.IGNORECASE).groupdict()

    meta['acquisitionMonthInteger'] = int(meta['acquisitionMonth'])
    meta['acquisitionDayInteger'] = int(meta['acquisitionDay'])

    return dict(scene_id=scene_id, **meta, **args)


def landsat(scene_id, args: Dict[str, Any] = dict()):
    pattern = (
        r"^L"
        r"(?P<sensor>\w{1})"
        r"(?P<satellite>\w{2})"
        r"_"
        r"(?P<processingCorrectionLevel>\w{4})"
        r"_"
        r"(?P<path>[0-9]{3})"
        r"(?P<row>[0-9]{3})"
        r"_"
        r"(?P<acquisitionYear>[0-9]{4})"
        r"(?P<acquisitionMonth>[0-9]{2})"
        r"(?P<acquisitionDay>[0-9]{2})"
        r"_"
        r"(?P<processingYear>[0-9]{4})"
        r"(?P<processingMonth>[0-9]{2})"
        r"(?P<processingDay>[0-9]{2})"
        r"_"
        r"(?P<collectionNumber>\w{2})"
        r"_"
        r"(?P<collectionCategory>\w{2})$"
    )
    meta: Dict[str, Any] = re.match(pattern, scene_id, re.IGNORECASE).groupdict()

    instruments = {
        '05': 'tm',
        '07': 'etm',
        '08': 'oli-tirs'
    }

    meta['instrument'] = instruments[meta['satellite']]

    return dict(scene_id=scene_id, **meta, **args)


class SceneParser:

    def __init__(self, group):
        parsers = dict(
            sentinel_2=sentinel_2,
            landsat=landsat
        )

        self.parser = parsers[group]

    def parser_sceneid(self, scene_id, args):
        return self.parser(scene_id, args=args)
