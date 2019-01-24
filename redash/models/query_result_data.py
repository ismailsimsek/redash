from .base import db, Column
import cPickle
from redash import settings
import os
import logging

logger = logging.getLogger(__name__)


class QueryResultData(object):

    def get(self, id, data_source_id):
        return None

    def delete(self, id, data_source_id):
        return True

    def save(self, id, data_source_id, data):
        return True


class QueryResultDbData(QueryResultData, db.Model):
    id = Column(db.Integer, primary_key=True, autoincrement=False, nullable=False)
    data = Column(db.Text, nullable=False, default='{}')

    __tablename__ = 'query_resultdata'

    def get(self, id, data_source_id):
        result = self.query.get(id)
        if result and result.data:
            return result.data
        else:
            return '{}'

    def delete(self, id, data_source_id):
        db.session.query(self).filter(self.id == id).delete()
        db.session.commit()
        return True

    def save(self, id, data_source_id, data):
        db.session.merge(QueryResultDbData(id=id, data=data or '{}'))
        db.session.commit()
        return True


class QueryResultFileData(QueryResultData):

    def get(self, id, data_source_id):
        data_file = self._get_file_path(id, data_source_id)

        if os.path.isfile(data_file):
            data = cPickle.load(open(data_file, "rb"))
        else:
            data = '{}';
        return data

    def delete(self, id, data_source_id):
        try:
            os.remove(self._get_file_path(id, data_source_id))
        except:
            pass
        return True

    def save(self, id, data_source_id, data):
        logging.error("dumping data" + str(data))
        print ("dumping data" + str(data))
        print ("dumping data" + str(self._get_file_path(id, data_source_id)))
        cPickle.dump(data, open(self._get_file_path(id, data_source_id), "wb"), protocol=2)
        return True

    def _get_file_path(self, id, data_source_id):
        result_directory = os.path.join(settings.QUERY_RESULTS_STORAGE_FILE_DIR, str(data_source_id))
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)
        return os.path.join(result_directory, str(id) + ".pkl")
