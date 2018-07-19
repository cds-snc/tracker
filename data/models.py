import typing
import pymongo
from pymongo import UpdateOne


# Data loads should clear the entire database first.
def _clear_collection(client: pymongo.MongoClient, name: str, database: typing.Optional[str] = None):
    client.get_database(database).get_collection('meta').delete_many({'_collection': name})


def _insert_all(
        client: pymongo.MongoClient,
        collection: str,
        documents: typing.Iterable[typing.Dict],
        database: typing.Optional[str] = None) -> None:
    client.get_database(database)\
          .get_collection('meta')\
          .insert_many({'_collection': collection, **document} for document in documents)


def _insert(
        client: pymongo.MongoClient,
        collection: str,
        document: typing.Dict,
        database: typing.Optional[str] = None) -> None:
    client.get_database(database).get_collection('meta').insert_one({'_collection': collection, **document})


def _upsert_all(
        client: pymongo.MongoClient,
        collection: str,
        documents: typing.Iterable[typing.Dict],
        key_col: str = '_id',
        database: typing.Optional[str] = None) -> None:

    writes = [
        UpdateOne(
            {'_collection': collection, key_col: document.get(key_col)},
            {'$set': {'_collection': collection, **document}},
            upsert=True,
        ) for document in documents
    ]

    client.get_database(database)\
          .get_collection('meta')\
          .bulk_write(writes)

def _replace(
        client: pymongo.MongoClient,
        collection: str,
        query: typing.Dict,
        document: typing.Dict,
        database: typing.Optional[str] = None) -> None:

    client.get_database(database)
          .get_collection('meta')
          .replace_one(({"_collection": collection, **query}, document)

def _find(
        client: pymongo.MongoClient,
        collection: str,
        query: typing.Dict,
        database: typing.Optional[str] = None) -> typing.Iterable[typing.Dict]:
    return client.get_database(database)\
                 .get_collection('meta')\
                 .find({'_collection': collection, **query}, {'_id': False, '_collection': False})


class _Collection():

    def __init__(self, client: pymongo.MongoClient, name: str) -> None:
        self._name = name
        self._client = client
        try:
            self._db = client.get_database().name
        except pymongo.errors.ConfigurationError:
            self._db = 'track'

    def create_all(self, documents: typing.Iterable[typing.Dict]) -> None:
        _insert_all(self._client, self._name, documents, self._db)

    def create(self, document: typing.Dict) -> None:
        _insert(self._client, self._name, document, self._db)

    def upsert_all(self, documents: typing.Iterable[typing.Dict], key_column: str) -> None:
        _upsert_all(self._client, self._name, documents, key_column, self._db)

    def replace(self, query: typing.Dict, document: typing.Dict) -> None:
        _replace(self._client, self._name, query, document, self._db)


    def all(self) -> typing.Iterable[typing.Dict]:
        return _find(self._client, self._name, {}, self._db)

    def clear(self) -> None:
        _clear_collection(self._client, self._name, self._db)


class Connection():

    def __init__(self, connection_string: str) -> None:
        self._client = pymongo.MongoClient(connection_string)

    def __enter__(self) -> 'Connection':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._client.close()

    @property
    def domains(self) -> _Collection:
        return _Collection(self._client, 'domains')

    @property
    def reports(self) -> _Collection:
        return _Collection(self._client, 'reports')

    @property
    def organizations(self) -> _Collection:
        return _Collection(self._client, 'organizations')

    @property
    def owners(self) -> _Collection:
        return _Collection(self._client, 'owners')

    @property
    def input_domains(self) -> _Collection:
        return _Collection(self._client, 'input_domains')

    @property
    def ciphers(self) -> _Collection:
        return _Collection(self._client, 'ciphers')

    @property
    def flags(self) -> _Collection:
        return _Collection(self._client, 'flags')

    def close(self) -> None:
        self._client.close()
