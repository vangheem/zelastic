from persistent.mapping import PersistentMapping
from BTrees.OOBTree import OOBTree
import uuid
from pyes import (ES, MatchAllQuery, FilteredQuery, TermFilter, ANDFilter)
from pyes.exceptions import IndexAlreadyExistsException

_storage_key = '__zelastic'
_meta_storage_key = '__meta__'


class BaseZelasticException(Exception):
    pass


class InvalidIndexException(BaseZelasticException):
    pass


class ElasticCatalog(object):
    doc_type = 'zelastic'
    default_indexes = {
        'zelastic_container_name' : {
            'type': 'string',
            'index': 'not_analyzed',
            'store': False
        },
        'zelastic_doc_key': {
            'type': 'string',
            'index': 'not_analyzed',
            'store': False
        }
    }

    def __init__(self, connection_string, elastic_name, storage, bulk=False,
                 bulk_size=400):
        self.conn = ES(connection_string, bulk_size=bulk_size)
        self.bulk_size = bulk_size
        self.name = elastic_name
        self.storage = storage
        self.bulk = bulk

    def update_mapping(self, name):
        meta = self.storage.meta(name)
        indexes = meta['indexes']
        properties = self.default_indexes.copy()
        try:
            self.conn.create_index(self.name)
        except IndexAlreadyExistsException:
            pass
        for index_name, _type in indexes.items():
            index = None
            if _type == 'str':
                index = {
                    'type': 'string',
                    'index': 'not_analyzed',
                    'store': False
                }
            elif _type == 'full':
                index = {
                    'type': 'string',
                    'index': 'analyzed',
                    'store': False
                }
            elif _type == 'bool':
                index = {
                    'type': 'boolean'
                }
            elif _type == 'int':
                index = {
                    'type': 'integer',
                    'store': False
                }
            elif _type == 'datetime':
                index = {
                    'type': 'date',
                    'store': False
                }
            elif _type == 'float':
                index = {
                    'type': 'float',
                    'store': False
                }
            if index is not None:
                properties[index_name] = index
        self.conn.indices.put_mapping(
            doc_type=self.doc_type,
            mapping={
                'properties': properties
            },
            indices=[self.name])

    def id(self, container_name, key):
        return '%s-%s' % (container_name, key)

    def index(self, container_name, doc, key):
        # need to add data to the index that isn't actually persisted
        data = {
            'zelastic_doc_key': key,
            'zelastic_container_name': container_name
        }
        meta = self.storage.meta(container_name)
        indexes = meta['indexes']
        for index in indexes.keys():
            if index in doc:
                data[index] = doc[index]
        self.conn.index(data, self.name, self.doc_type,
            self.id(container_name, key), bulk=self.bulk)

    def delete(self, container_name, key):
        self.conn.delete(self.name, self.doc_type,
            self.id(container_name, key))


class Storage(object):

    def __init__(self, root, es_string, es_name, bulk=False, bulk_size=400):
        self.es = ElasticCatalog(es_string, es_name, self, bulk=bulk,
                bulk_size=bulk_size)
        self.root = root
        if _storage_key not in self.root:
            self.root[_storage_key] = PersistentMapping()
        self.store = self.root[_storage_key]

    def container(self, name):
        if name not in self.store:
            self.store[name] = OOBTree()
        return Container(self, name)

    def drop(self, name):
        if name in self.store:
            del self.store[name]

    def list(self):
        res = self.store.keys()
        if _meta_storage_key in res:
            res.remove(_meta_storage_key)
        return res

    def meta(self, name):
        if _meta_storage_key not in self.store:
            self.store[_meta_storage_key] = PersistentMapping()
        meta = self.store[_meta_storage_key]
        if name not in meta:
            meta[name] = PersistentMapping()
            meta[name]['indexes'] = PersistentMapping()
        return meta[name]


class ResultWrapper(object):
    def __init__(self, container, rl):
        self.container = container
        self.rl = rl

    def __getitem__(self, val):
        elasticres = self.rl[val]
        if hasattr(elasticres, '__iter__'):
            return [
                self.container.get(r.zelastic_doc_key)
                for r in elasticres]
        else:
            return self.container.get(elasticres.zelastic_doc_key)

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.rl)


class Container(object):

    def __init__(self, store, name):
        self.store = store
        self._data = store.store[name]
        self.name = name
        self.es = self.store.es

    def insert(self, data, key=None):
        if key is None:
            key = str(uuid.uuid4())
            while key in self._data:
                key = str(uuid.uuid4())
        else:
            if key in self._data:
                # already have this key, error
                raise KeyError('The key "%s" already exists in database' % (
                    key))
        self._data[key] = data
        self.es.index(self.name, data, key)
        return key

    def update(self, data, key):
        if key not in self._data:
            raise KeyError('Update failed: The key "%s" ' % key + \
                           'does not exist in database.')
        self._data[key] = data
        self.es.index(self.name, data, key)

    def delete(self, key):
        if key not in self._data:
            raise KeyError('Delete failed: The key "%s" ' % key + \
                           'does not exist in database.')
        del self._data[key]
        self.es.delete(self.name, key)

    def get(self, key):
        if key not in self._data:
            raise KeyError('get failed: The key "%s" ' % key + \
                           'does not exist in database.')
        return self._data[key]

    def add_index(self, index_name, _type):
        meta = self.store.meta(self.name)
        index = meta['indexes']
        # validate supported types
        if _type not in ('int', 'float', 'str', 'full', 'datetime', 'bool'):
            raise InvalidIndexException('The index type "%s" is not valid' % (
                getattr(_type, '__name__', '')))
        index[index_name] = _type
        self.es.update_mapping(self.name)

    def search(self, dquery={}, sort='zelastic_doc_key'):
        filters = []
        query = MatchAllQuery()
        for key, value in dquery.items():
            filters.append(TermFilter(key, value))
        filters.append(TermFilter('zelastic_container_name', self.name))
        query = FilteredQuery(query, ANDFilter(filters))
        return ResultWrapper(
            self,
            self.es.conn.search(query=query, fields="zelastic_doc_key",
                sort=sort))

