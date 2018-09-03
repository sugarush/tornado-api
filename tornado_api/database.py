import json

import rethinkdb as r


r.set_loop_type('asyncio')


class Connection(object):

    connections = { }

    @classmethod
    def _hash(cls, kargs):
        return json.dumps(kargs, separators=(',', ':'), sort_keys=True)

    @classmethod
    async def connect(cls, **kargs):
        key = cls._hash(kargs)

        connection = cls.connections.get(key)
        if connection and connection.is_open():
            return connection

        cls.connections[key] = await r.connect(**kargs)
        return cls.connections[key]

    @classmethod
    async def close(cls):
        for key in cls.connections:
            connection = cls.connections[key]
            if connection.is_open():
                await connection.close()

database = Connection
