from gateway import get_gateway
from py4j import java_collections
from collections import OrderedDict

class OrcReader:
    def __init__(self, path):
        self.gateway = get_gateway()
        self.path = path
        self.reader = None

    def open(self):
        try:
            gateway = self.gateway
            reader = gateway.jvm.com.pythonorc.gateway.SimplifiedOrcReader(self.path)
            reader.open()

            self.reader = reader
        except Exception:
            if self.reader:
                self.reader._detach()
                self.reader = None
            raise

    def close(self):
        self.reader.close()

        self.reader._detach()
        self.reader = None

    @property
    def num_rows(self):
        return self.reader.getNumberOfRows()

    @property
    def num_cols(self):
        raise

    def batch(self, size=1024):
        return

    def schema(self):
        fields = self.reader.getFieldNames()
        dict = self.reader.getSchemaDictionary()
        return OrderedDict([(field, dict[field]) for field in fields])

    def __iter__(self):
        return OrcRecordIterator(self.reader.iterator())


class OrcRecordIterator:
    def __init__(self, iterator):
        self.iterator = iterator

    def next(self):
        if not self.iterator.hasNext():
            self.iterator._detach()
            raise StopIteration()

        record = self.iterator.next()
        py_record = list(record)

        for i in range(0, len(py_record)):
            if type(py_record[i]) is java_collections.JavaArray:
                arr = list(py_record[i])
                py_record[i]._detach()
                py_record[i] = arr

        record._detach()
        return py_record
