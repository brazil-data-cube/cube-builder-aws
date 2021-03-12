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