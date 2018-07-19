import itertools
import logging
from time import sleep
import typing
import pymongo
from pymongo import UpdateOne


class TrackerModelError(Exception):
    pass

class InsertionError(TrackerModelError):
    def __init__(self, *args, errors, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = errors

class DeleteError(TrackerModelError):
    pass


def grouper(group_size, iterable):
    iterator = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(iterator, group_size))
        if not chunk:
            return
        yield chunk


LOGGER = logging.getLogger(__name__)
MAX_TRIES = 5

REQUEST_RATE_ERROR = 16500
DUPLICATE_KEY_ERROR = 11000
DELETE_REFUSED = 2

def _retry_delete_in(
        objects: typing.Sequence,
        method: typing.Callable[[dict], None],
        times: int,
    ) -> None:

    query = {
        "_id": {
            "$in": objects
        }
    }

    for count in range(1, times+1):
        try:
            method(query)
            break
        except pymongo.errors.OperationFailure as exc:
            if exc.code == DELETE_REFUSED:
                # Request was too big, it's not going to do it.
                length = len(objects)
                if length == 1:
                    # We tried cutting up the data until it was a single write request, and that still failed
                    # time to pack it in
                    raise

                LOGGER.warning('Request refused, trying with half the previous request')

                iter_data = iter(objects)
                first_half = list(itertools.islice(iter_data, length//2))
                second_half = list(iter_data)
                _retry_delete_in(first_half, method, times)
                _retry_delete_in(second_half, method, times)
            else:
                raise
        except Exception as exc:
            raise
    else:
        # Loop exited normally, not via a break. This means that it failed each time
        raise DeleteError("Unable to execute request, failed %d times" % count)


def _retry_write(
        data: typing.Sequence[dict],
        write_method: typing.Callable[[typing.Iterable[dict]], None],
        times: int
    ) -> None:
    '''Attempt `write_method`(`data`) `times` times'''

    errors = []
    for count in range(1, times+1): # Only do {times} attempts to insert
        try:
            write_method(data)
            break
        except pymongo.errors.DuplicateKeyError as exc:
            # After retrying the insertion, some of the documents were duplicates, this is OK
            break
        except pymongo.errors.BulkWriteError as exc:
            details = exc.details.get('writeErrors', [])
            # Check if all errors were duplicate key errors, if so this is OK
            if not all(error['code'] == DUPLICATE_KEY_ERROR for error in details):
                raise
            break
        except pymongo.errors.OperationFailure as exc:
            # Check if we blew the request rate, if so take a break and try again
            errors.append(exc)
            if exc.code == REQUEST_RATE_ERROR:
                LOGGER.warning('Exceeded RU limit, pausing for %d seconds...', count)
                sleep(count)
            else:
                raise
        except Exception as exc:
            raise
    else:
        # Loop exited normally, not via a break. This means that it failed each time
        raise InsertionError("Unable to execute request, failed %d times" % count, errors=errors)

# Data loads clear the entire database first.
def _clear_collection(
        client: pymongo.MongoClient,
        name: str,
        database: typing.Optional[str] = None,
        batch_size: typing.Optional[int] = None) -> None:
    if not batch_size:
        client.get_database(database).get_collection('meta').delete_many({'_collection': name})
    else:
        collection = client.get_database(database).get_collection('meta')

        # In order to chunk up a delete request, we need to first do a find
        # for the documents we want to delete
        cursor = collection.find({'_collection': name}, {"_id": True})
        chunks = (
            [doc["_id"] for doc in chunk]
            for chunk in grouper(batch_size, cursor)
        )
        for object_ids in chunks:
            _retry_delete_in(object_ids, collection.delete_many, MAX_TRIES)


def _insert_all(
        client: pymongo.MongoClient,
        collection: str,
        documents: typing.Iterable[typing.Dict],
        database: typing.Optional[str] = None,
        batch_size: typing.Optional[int] = None) -> None:
    if not batch_size:
        client.get_database(database)\
              .get_collection('meta')\
              .insert_many([{'_collection': collection, **document} for document in documents])
    else:
        document_stream = grouper(batch_size, documents)
        collect = client.get_database(database).get_collection('meta')
        for chunk in document_stream:
            documents = [{'_collection': collection, **document} for document in chunk]
            _retry_write(documents, collect.insert_many, MAX_TRIES)


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
        database: typing.Optional[str] = None,
        batch_size: typing.Optional[int] = None) -> None:

    writes = (
        UpdateOne(
            {'_collection': collection, key_col: document.get(key_col)},
            {'$set': {'_collection': collection, **document}},
            upsert=True,
        ) for document in documents
    )

    if not batch_size:
        client.get_database(database)\
              .get_collection('meta')\
              .bulk_write(list(writes))
    else:
        document_stream = grouper(batch_size, writes)
        collect = client.get_database(database).get_collection('meta')
        for chunk in document_stream:
            to_write = [write for write in chunk]
            _retry_write(to_write, collect.bulk_write, MAX_TRIES)

def _replace(
        client: pymongo.MongoClient,
        collection: str,
        query: typing.Dict,
        document: typing.Dict,
        database: typing.Optional[str] = None) -> None:

    client.get_database(database)\
          .get_collection('meta')\
          .replace_one({"_collection": collection, **query}, {"_collection": collection, **document})

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

    def create_all(self, documents: typing.Iterable[typing.Dict], batch_size: typing.Optional[int] = None) -> None:
        _insert_all(self._client, self._name, documents, self._db, batch_size)

    def create(self, document: typing.Dict) -> None:
        _insert(self._client, self._name, document, self._db)

    def upsert_all(self,
                   documents: typing.Iterable[typing.Dict],
                   key_column: str,
                   batch_size: typing.Optional[int] = None
                  ) -> None:
        _upsert_all(self._client, self._name, documents, key_column, self._db, batch_size)

    def replace(self, query: typing.Dict, document: typing.Dict) -> None:
        _replace(self._client, self._name, query, document, self._db)


    def all(self) -> typing.Iterable[typing.Dict]:
        return _find(self._client, self._name, {}, self._db)

    def clear(self, batch_size: typing.Optional[int] = None) -> None:
        _clear_collection(self._client, self._name, self._db, batch_size)


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
