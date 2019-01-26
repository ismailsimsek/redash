from .base import db, Column
import cPickle
from redash import settings
import os
import logging
from redash.models import QueryResult
import uuid
from __future__ import generators
import os.lin
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from redash.utils import json_dumps, json_loads

logger = logging.getLogger(__name__)


class QueryResultData(object):
    data = None
    data_handler = "db"

    def __init__(self, data = None):
        self.data = data

    def get(self):
        return self.data

    def delete(self):
        return True

    def save(self, cursor, columns):
        return None

class QueryResultDbData(QueryResultData):

    data_handler = "db"

    def __init__(self, data = None):
        self.data = data

    def get(self):
        return self.data

    def delete(self):
        return True

    def save(self, cursor, columns):
        # fetchall and return json data! backward compatibe behaviour
        # copy of mysql.py
        json_data= None
        data = cursor.fetchall()
        while cursor.nextset():
            data = cursor.fetchall()
        if cursor.description is not None:
            rows = [dict(zip((c['name'] for c in columns), row)) for row in data]
            data = {'columns': columns, 'rows': rows}
            json_data = json_dumps(data)
        return json_data

class QueryResultFileData(QueryResultData):

    data = None
    filename = str(uuid.uuid4())
    data_handler = "file"
    sample_row_limit = 50

    def __init__(self, data = None):
        if data is None:
            self.filename = str(uuid.uuid4())+".json"
            _data={'filename': self.filename, 'data_handler':self.data_handler,
                   'count': 0, 'sampe_rows':{}, 'columns':{}
                   }
            self.data = json_dumps(_data)
        else:
            self.data = data
            self.filename = data.filename

    def delete(self):
        try:
            os.remove(self._get_file_path())
        except:
            pass
        return True

    def save(self,cursor,columns):
        # @TODO stream to file! loop rows and append to file!
        json_data = None
        count=0

        data = cursor.fetchall()
        while cursor.nextset():
            data = cursor.fetchall()
        if cursor.description is not None:
            rows = [dict(zip((c['name'] for c in columns), row)) for row in data]
            count = len(rows)
            data = {'columns': columns, 'rows': rows}
            json_data = json_dumps(data)

        cPickle.dump(json_data, open(self._get_file_path(), "wb"), protocol=2)

        _data={'filename': self.filename, 'data_handler':self.data_handler,
                   'count': count, 'sampe_rows':{}, 'columns':columns
                   }

        return _data

    def get(self):
        data_file = self._get_file_path()
        if os.path.isfile(data_file):
            data = cPickle.load(open(data_file, "rb"))
        else:
            data = '{}';
        return data
    
    # def get_as_csv_stream(self): ?????
    # def get_as_excel_stream(self): ?????
    # def get_as_json_stream(self): ?????
    # def get_as_stream(self):
    #    # @TODO loop read file and return full data
    #    row = None
    #    yield row

    # def resultIter(cursor, arraysize=1000):
    #    'An iterator that uses fetchmany to keep memory usage down'
    #    while True:
    #        results = cursor.fetchmany(arraysize)
    #        if not results:
    #            break
    #        for result in results:
    #            yield result

    def _get_file_path(self):
        if not os.path.exists(settings.QUERY_RESULTS_STORAGE_FILE_DIR):
            os.makedirs(settings.QUERY_RESULTS_STORAGE_FILE_DIR)
        return os.path.join(settings.QUERY_RESULTS_STORAGE_FILE_DIR, str(self.data.filename))

class QueryResultDataFactory(object):
    @staticmethod
    def get_handler(self,data_handler=None,**kwargs):
        result_data_handlers = {
            "db": QueryResultDbData,
            "file": QueryResultFileData,
            # "s3": QueryResultS3Data  @TODO
        }

        if not data_handler:
            data_handler = settings.QUERY_RESULTS_STORAGE_TYPE or 'db'
        # set storage type for query_result.data object
        __data_handler_class = result_data_handlers.get(self.data_handler)
        # init the class responsible of handling result_data
        logging.info("Using (%s) class as result data handler!", __data_handler_class)
        return __data_handler_class(**kwargs)

