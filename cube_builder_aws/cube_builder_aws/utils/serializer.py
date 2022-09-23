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

import json
from decimal import Decimal

from sqlalchemy.inspection import inspect


class Serializer(object):
    @staticmethod
    def serialize(obj):
        result = dict()
        for c in inspect(obj).mapper.column_attrs:
            value = getattr(obj, c.key)
            if type(value) == Decimal:
                value = float(value)
            result[c.key] = value
        return result

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o) if '.' in str(o) else int(o)
        return super(DecimalEncoder, self).default(o)