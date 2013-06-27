try:
    import unittest2 as unittest
except ImportError:
    import unittest
from zelastic import Storage
import zelastic


class ZelasticTests(unittest.TestCase):

    def setUp(self):
        self.root = {}
        self.storage = Storage(self.root, 'http://127.0.0.1:9200', 'testing')

    def tearDown(self):
        self.storage.es.conn.delete_index_if_exists('testing')
        self.storage.es.conn.refresh()

    def test_create_mapping_on_new_container(self):
        container = self.storage.container('foobar')
        mapping = container.es.conn.get_mapping('foobar', 'testing')
        assert 'zelastic_doc_id' in mapping['foobar']['properties']

    def test_add_container_adds_data(self):
        self.storage.container('foobar')
        assert zelastic._storage_key in self.root
        assert 'foobar' in self.root[zelastic._storage_key]

    def test_adding_data(self):
        container = self.storage.container('foobar')
        data = {'foo': 'bar'}
        container.insert(data, 'foobar')
        assert data == container.get('foobar')

    def test_added_data_indexed(self):
        container = self.storage.container('foobar')
        data = {'foo': 'bar'}
        container.insert(data, 'foobar')
        container.es.conn.refresh()
        result = container.search()[0]
        assert data == result

    def test_adding_data_with_same_id_raises_error(self):
        container = self.storage.container('foobar')
        data = {'foo': 'bar'}
        container.insert(data, 'foobar')
        self.assertRaises(KeyError, container.insert, data, 'foobar')

    def test_adding_data_with_no_id_creates_random(self):
        container = self.storage.container('foobar')
        data = {'foo': 'bar'}
        id = container.insert(data)
        assert id is not None

    def test_adding_index(self):
        container = self.storage.container('foobar')
        container.add_index('foo', 'str')
        container.es.conn.refresh()
        mapping = container.es.conn.get_mapping('foobar', 'testing')
        assert 'foo' in mapping['foobar']['properties']

    def test_index_searchable(self):
        container = self.storage.container('foobar')
        container.add_index('foo', 'str')
        data = {'foo': 'bar'}
        container.insert(data, 'foobar')
        container.es.conn.refresh()
        result = container.search(foo='bar')[0]
        assert data == result


if __name__ == '__main__':
    unittest.main()

