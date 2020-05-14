from sqlalchemy.inspection import inspect


class Serializer(object):
    @staticmethod
    def serialize(obj):
        return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]
