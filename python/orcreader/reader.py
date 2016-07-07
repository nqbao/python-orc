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
    def __init__(self, path, gateway=None):
        self.gateway = gateway
        self._custom_gateway = None
        self.path = path
        self.reader = None

    def open(self):
        reader = None
        try:
            gateway = self.gateway

            if not gateway:
                gateway = self.gateway = get_gateway()
                self._custom_gateway = True

            reader = gateway.jvm.com.pythonorc.SimplifiedOrcReader(self.path)
            reader.open()
        except Exception:
            if reader:
                reader._detach()

            self._shutdown_gateway()

            raise
        finally:
            self.reader = reader

    def close(self):
        # destroy the reader
        if self.reader:
            self.reader.close()
            self.reader._detach()

        self._shutdown_gateway()

        self.reader = None

    def _shutdown_gateway(self):
        # destroy the gateway
        if self._custom_gateway:
            self.gateway.shutdown()
            self.gateway = None
            self._custom_gateway = False

    @property
    def num_rows(self):
        return self.reader.getNumberOfRows()

    @property
    def num_cols(self):
        return len(self.schema())

    def batch(self, size=1024):
        return OrcRecordIterator(self.reader.batch(size))

    def schema(self):
        fields = self.reader.getFieldNames()
        dict = self.reader.getSchemaDictionary()
        return OrderedDict([(field, dict[field]) for field in fields])

    def __iter__(self):
        return OrcRecordIterator(self.reader.iterator())

    def __enter__(self):
        self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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
