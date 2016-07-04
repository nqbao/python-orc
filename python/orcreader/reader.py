from gateway import get_gateway
from py4j import java_collections
from collections import OrderedDict


def normalize_record(record):
    if type(record) is java_collections.JavaArray:
        items = [normalize_record(item) for item in record]
        record._detach()
        return items
    elif type(record) is java_collections.JavaMap:
        items = {field: normalize_record(record[field]) for field in record}

        record._detach()
        return items

    return record


class OrcReader:
    def __init__(self, path):
        self.gateway = get_gateway()
        self.path = path
        self.reader = None

    def open(self):
        try:
            gateway = self.gateway
            reader = gateway.jvm.com.pythonorc.SimplifiedOrcReader(self.path)
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
        return OrcRecordIterator(self.reader.batch(size))

    def schema(self):
        fields = self.reader.getFieldNames()
        dict = self.reader.getSchemaDictionary()
        return OrderedDict([(field, dict[field]) for field in fields])

    def __iter__(self):
        return OrcRecordIterator(self.reader.iterator())


class OrcRecordIterator:
    def __init__(self, iterator):
        self.iterator = iterator

    def __iter__(self):
        return self

    def next(self):
        if not self.iterator.hasNext():
            self.iterator._detach()
            raise StopIteration()

        return normalize_record(self.iterator.next())

