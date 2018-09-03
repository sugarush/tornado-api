import inspect
import inflection

from . database import database


class Field(object):

    def __init__(self, type=str, primary=False, required=False, related=False, indexed=False, computed=False, computed_empty=False, computed_type=False):
        self.name = None
        self.type = type
        self.primary = primary
        self.required = required
        self.related = related
        self.indexed = indexed
        self.computed = computed
        self.computed_empty = computed_empty
        self.computed_type = computed_type

        self.updated = False

        if self.primary:
            self.required = True

        if issubclass(self.type, Model):
            self.required = True

    def __repr__(self):
        message = '<Field name:{name} type:{type} primary:{primary} required:{required} related:{related} indexed:{indexed} computed:{computed}>'
        return message.format(
            name=self.name,
            type=self.type.__name__,
            primary=self.primary,
            required=self.required,
            related=self.related,
            indexed=self.indexed,
            computed=self.computed
        )


class ModelMeta(type):

    def __init__(cls, name, bases, attrs):
        super(ModelMeta, cls).__init__(name, bases, attrs)

        if cls.__name__ in ('Model', 'RethinkDBModel'):
            return

        cls._table = inflection.tableize(cls.__name__)

        cls._fields = [ ]
        cls._nested = [ ]
        cls._related = [ ]
        cls._required = [ ]
        cls._indexed = [ ]
        cls._computed = [ ]

        members = inspect.getmembers(cls, lambda f: isinstance(f, Field))

        for name, field in members:

            field.name = name

            if issubclass(field.type, Model):
                if field.related:
                    cls._related.append(field)
                else:
                    cls._nested.append(field)

            if field.required:
                cls._required.append(field)

            if field.indexed:
                cls._indexed.append(field)

            if field.computed:
                cls._computed.append(field)

            cls._fields.append(field)

        primary = list(filter(lambda field: field.primary, cls._fields))

        if len(primary) > 1:
            fields = list(map(lambda field: field.name, primary))
            message = "Model {model} has multiple primary fields: {fields}".format(
                model=cls.__name__,
                fields=fields
            )
            raise Exception(message)

        if not primary:
            field = Field()
            field.name = 'id'
            field.primary = True
            cls.id = field
            cls._fields.append(field)
            primary.append(field)

        cls._primary = primary[0]


class Model(object, metaclass=ModelMeta):

    def __init__(self, dictionary=None, **kargs):
        self._check_computed(self._computed)
        self.set(dictionary, **kargs)

    def __repr__(self):
        return repr(self.serialize())

    def __setattr__(self, name, value):
        self._set(name, value)

    def _check_computed(self, fields):
        methods = list(filter(lambda field: isinstance(field.computed, str), fields))

        missing = list(filter(lambda field: not hasattr(self, field.computed), methods))

        if len(missing) > 0:
            names = list(map(lambda field: field.computed, missing))
            message = 'Model {model} has missing methods: {fields}'.format(
                model=self.__class__.__name__,
                fields=names
            )
            raise AttributeError(message)

        for field in methods:
            field.computed = getattr(self, field.computed)

        invalid = list(filter(lambda field: not (inspect.isfunction(field.computed) or inspect.ismethod(field.computed)), fields))

        if len(invalid) > 0:
            names = list(map(lambda field: field.computed, invalid))
            message = 'Model {model} computed fields must be method names or functions: {fields}'.format(
                model=self.__class__.__name__,
                fields=names
            )
            raise AttributeError(message)

    def _check_missing(self, kargs):
        missing = list(map(lambda field: field.name, filter(lambda field: field.name not in self.__dict__, self._required)))

        if len(missing) > 0:
            message = 'Model {model} has missing fields: {fields}'.format(
                model=self.__class__.__name__,
                fields=missing
            )
            raise AttributeError(message)

    def _check_undefined(self, kargs):
        undefined = list(filter(lambda karg: karg not in map(lambda field: field.name, self._fields), kargs))

        if len(undefined) > 0:
            message = 'Model {model} has undefined fields: {fields}'.format(
                model=self.__class__.__name__,
                fields=undefined
            )
            raise AttributeError(message)

    def _check_field(self, key):
        field = list(filter(lambda field: field.name == key, self._fields))

        if not field:
            message = 'Model {model} does not have field: {field}'.format(
                model=self.__class__.__name__,
                field=key
            )
            raise AttributeError(message)

        return field[0]

    def _set(self, key, value):
        field = self._check_field(key)
        if not type(field.type) == type:
            if type(value) == dict:
                self.__dict__[key] = field.type(**value)
            else:
                self.__dict__[key] = value
        else:
            self.__dict__[key] = field.type(value)

    def set(self, _dictionary=None, **kargs):
        if not _dictionary:
            _dictionary = { }

        _dictionary.update(kargs)
        kargs = _dictionary

        self._check_undefined(kargs)

        for karg in kargs:
            self._set(karg, kargs[karg])

    def serialize(self, verify=False):
        if verify:
            self._check_missing(self.__dict__)

        obj = self.__dict__.copy()

        for field in self._fields:
            if issubclass(field.type, Model):
                obj[field.name] = self.__dict__[field.name].serialize(verify)
            elif field.computed:
                if field.computed_empty:
                    continue
                if field.computed_type:
                    obj[field.name] = field.computed()
                else:
                    obj[field.name] = field.type(field.computed())

        return obj
