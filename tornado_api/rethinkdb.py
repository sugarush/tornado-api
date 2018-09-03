import rethinkdb as r

from . database import database
from . model import Model

class RethinkDBModel(Model):

    db_options = { }
    table_options = { }

    connection = None

    r = None
    _db = None

    _ensure = True

    @classmethod
    async def connect(cls):
        if not cls.connection or not cls.connection.is_open():
            cls.connection = await database.connect(**cls.db_options)

        if cls._ensure:
            await cls._ensure_database()
            await cls._ensure_table()
            await cls._ensure_indexes()
            cls._ensure = False

    @classmethod
    async def close(cls):
        if cls.connection and cls.connection.is_open():
            await cls.connection.close()

    @classmethod
    async def _ensure_database(cls):
        databases = await r.db_list().run(cls.connection)
        db = cls.db_options.get('db', 'test')
        if not db in databases:
            await r.db_create(db).run(cls.connection)
        cls._db = r.db(db)

    @classmethod
    async def _ensure_table(cls):
        tables = await cls._db.table_list().run(cls.connection)
        cls.table_options['primary_key'] = cls._primary.name
        if not cls._table in tables:
            await cls._db.table_create(cls._table, **cls.table_options).run(cls.connection)
        cls.r = cls._db.table(cls._table)

    @classmethod
    async def _ensure_indexes(cls):
        pass

    @classmethod
    async def drop(cls):
        await cls.connect()
        tables = await cls._db.table_list().run(cls.connection)
        if cls._table in tables:
            await cls._db.table_drop(cls._table).run(cls.connection)
            cls._ensure = True

    @classmethod
    async def read(cls, id):
        await cls.connect()
        result = await cls.r.get(id).run(cls.connection)
        if result:
            return cls(result)

    async def create(self):
        await self.connect()
        result = await self.r.insert(self.serialize(verify=True)).run(self.connection)
        print(result)

    async def update(self):
        await self.connect()
        result = self.r.get(self.__dict__[self._primary.name]).update(self.serialize()).run(self.connection)

    async def delete(self):
        await self.connect()
        result = await self.r.get(self.__dict__[self._primary.name]).delete().run(self.connection)
        print(result)
