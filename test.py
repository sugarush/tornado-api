from unittest import skip

import rethinkdb as r

from tornado_api import AsyncTestCase, Model, Field, RethinkDBModel, database


class DatabaseTest(AsyncTestCase):

    def test_hash(self):
        expected = '{"a":"a","b":{"d":"d","e":"e"},"c":"c"}'
        result = database._hash({
            'c': 'c',
            'a': 'a',
            'b': { 'e': 'e', 'd': 'd' }
        })
        self.assertEqual(expected, result)

    async def test_connect(self):
        c = await database.connect(db='test', ssl={'cacerts': ''})
        self.assertTrue(c.is_open())
        await database.close()

    async def test_connect_multiple(self):
        a = await database.connect(db='test')
        b = await database.connect(db='test')
        c = await database.connect(db='testing')
        self.assertIs(a, b)
        self.assertIsNot(a, c)
        await database.close()

    async def test_close(self):
        a = await database.connect(db='test')
        b = await database.connect(db='testing')
        await database.close()
        self.assertFalse(a.is_open())
        self.assertFalse(b.is_open())


class ModelMetaTest(AsyncTestCase):

    def test_field_primary_default(self):

        class Test(Model):
            pass

        self.assertTrue(Test.id)
        self.assertTrue(Test.id.primary)
        self.assertFalse(Test.id.required)
        self.assertIs(Test._primary.name, 'id')
        self.assertIn(Test._primary, Test._fields)

    def test_field_primary_custom(self):

        class Test(Model):
            field = Field(primary=True)

        self.assertTrue(Test.field.primary)
        self.assertTrue(Test.field.required)
        self.assertIs(Test._primary.name, 'field')
        with self.assertRaises(AttributeError):
            Test.id

    def test_field_primary_multiple(self):

        with self.assertRaises(Exception):

            class Test(Model):
                alpha = Field(primary=True)
                beta = Field(primary=True)

    def test_field_model_nested(self):

        class Beta(Model):
            pass

        field = Field(type=Beta)

        class Alpha(Model):
            beta = field

        self.assertIn(field, Alpha._nested)

    def test_field_model_related(self):

        class Beta(Model):
            pass

        field = Field(type=Beta, related=True)

        class Alpha(Model):
            beta = field

        self.assertIn(field, Alpha._related)

    def test_field_required(self):

        field = Field(required=True)

        class Test(Model):
            test = field

        self.assertIn(field, Test._required)

    def test_field_indexed(self):

        field = Field(indexed=True)

        class Test(Model):
            test = field

        self.assertIn(field, Test._indexed)


class ModelTest(AsyncTestCase):

    def test_check_computed_method(self):

        class Test(Model):
            computed = Field(computed='method')

            def method(self):
                pass

        test = Test()
        field = test._check_field('computed')

        self.assertTrue(callable(field.computed))

    def test_check_computed_missing(self):

        class Test(Model):
            computed = Field(computed='missing_method')

        with self.assertRaises(AttributeError):
            test = Test()

    def test_check_computed_invalid(self):

        class Test(Model):
            computed = Field(computed='value')

            value = 'test'

        with self.assertRaises(AttributeError):
            test = Test()

    def test_check_undefined(self):

        class Test(Model):
            pass

        test = Test()

        with self.assertRaises(AttributeError):
            test._check_undefined({ 'field': 'value' })

    def test_check_missing(self):

        class Test(Model):
            field = Field(required=True)

        test = Test()

        with self.assertRaises(AttributeError):
            test._check_missing({ })

    def test_check_field(self):

        class Test(Model):
            field = Field()

        test = Test()

        field = test._check_field('field')
        self.assertTrue(field)

        with self.assertRaises(AttributeError):
            test._check_field('undefined')

    def test_setattr(self):

        class Test(Model):
            field = Field()

        test = Test()
        test.field = 'value'

        self.assertIs(test.field, 'value')

    def test_set(self):

        class Test(Model):
            field = Field()

        test = Test()
        test._set('field', 'value')

        self.assertIs(test.field, 'value')

    def test_set_undefined(self):

        class Test(Model):
            pass

        test = Test()

        with self.assertRaises(AttributeError):
            test._set('field', 'value')

    def test_set_nested_model_from_dict(self):

        class Beta(Model):
            field = Field()

        class Alpha(Model):
            beta = Field(type=Beta)

        alpha = Alpha()
        alpha._set('beta', { 'field': 'value' })

        self.assertIs(alpha.beta.field, 'value')

    def test_set_nested_model_from_model(self):
        class Beta(Model):
            field = Field()

        class Alpha(Model):
            beta = Field(type=Beta)

        alpha = Alpha()
        alpha._set('beta', Beta({ 'field': 'value' }))

        self.assertIs(alpha.beta.field, 'value')

    def test_set_multiple_nested(self):

        class Gamma(Model):
            field = Field()

        class Beta(Model):
            gamma = Field(type=Gamma)

        class Alpha(Model):
            beta = Field(type=Beta)

        alpha = Alpha()
        alpha._set('beta', { 'gamma': { 'field': 'value' } })

        self.assertIs(alpha.beta.gamma.field, 'value')

    @skip(reason='implement later')
    def test_set_related(self):
        pass

    def test_set_keyword(self):

        class Test(Model):
            field = Field()

        test = Test()
        test.set(field='value')

        self.assertIs(test.field, 'value')

    def test_set_dictionary(self):

        class Test(Model):
            field = Field()

        test = Test()
        test.set({ 'field': 'value' })

        self.assertIs(test.field, 'value')

    def test_set_dictionary_and_keyword(self):

        class Test(Model):
            alpha = Field()
            beta = Field()

        test = Test()
        test.set({ 'alpha': 'a', 'beta': 'b' }, beta='value')

        self.assertIs(test.alpha, 'a')
        self.assertIs(test.beta, 'value')

    def test_serialize(self):

        expected = {'value': 'string', 'object': {'alpha': 'a', 'beta': 'b'}}

        class Test(Model):
            value = Field()
            object = Field(type=dict)

        test = Test()
        test.value = 'string'
        test.object = { 'alpha': 'a', 'beta': 'b' }

        self.assertEqual(test.serialize(), expected)

    def test_serialize_nested_model(self):

        expected = {'beta': {'value': 'test'}}

        class Beta(Model):
            value = Field()

        class Alpha(Model):
            beta = Field(type=Beta)

        alpha = Alpha()
        alpha.beta = { 'value': 'test' }

        self.assertEqual(alpha.serialize(), expected)

    def test_serialize_computed_function(self):

        class Test(Model):
            computed = Field(type=str, computed=lambda: 'value')

        test = Test()
        self.assertIs(test.serialize()['computed'], 'value')

    def test_serialize_computed_method(self):

        class Test(Model):
            computed = Field(type=str, computed='get_hello')

            def get_hello(self):
                return 'hello'

        test = Test()
        self.assertIs(test.serialize()['computed'], 'hello')

    def test_serialize_computed_empty(self):

        class Test(Model):
            computed = Field(type=str, computed='get_hello', computed_empty=True)

            def get_hello(self):
                return 'hello'

        test = Test()
        test.computed = 'value'

        self.assertIs(test.serialize()['computed'], 'value')

    def test_serialize_computed_type(self):

        class Test(Model):
            computed = Field(type=dict, computed='get_hello', computed_type=True)

            def get_hello(self):
                return 'hello'

        test = Test()

        with self.assertRaises(ValueError):
            test.computed = 'value'

        self.assertIs(test.serialize()['computed'], 'hello')

        
class RethinkDBModelTest(AsyncTestCase):

    async def test_connect_and_close(self):

        class Test(RethinkDBModel):
            db_options = { 'db': 'testdb' }

        await Test.connect()
        a = Test.connection

        await Test.connect()
        b = Test.connection

        self.assertTrue(Test.connection)
        self.assertTrue(Test.connection.is_open())
        self.assertIs(a, b)

        await Test.drop()
        await Test.close()

        self.assertFalse(Test.connection.is_open())

    async def test_ensure_database(self):

        class Test(RethinkDBModel):
            db_options = { 'db': 'testdb' }

        await Test.connect()

        databases = await r.db_list().run(Test.connection)

        self.assertIn('testdb', databases)

        await Test.drop()
        await r.db_drop('testdb').run(Test.connection)
        await Test.close()

    async def test_ensure_table(self):

        class Test(RethinkDBModel):
            pass

        await Test.connect()

        tables = await Test._db.table_list().run(Test.connection)

        self.assertIn(Test._table, tables)

        await Test.drop()
        await Test.close()

    async def test_ensure_indexes(self):

        class Beta(RethinkDBModel):
            field = Field(indexed=True)

        class Alpha(RethinkDBModel):
            beta = Field(type=Beta)
            field = Field(indexed=True)

    async def test_create(self):
        pass

    async def test_read(self):
        pass

    async def test_update(self):
        pass

    async def test_delete(self):
        pass
