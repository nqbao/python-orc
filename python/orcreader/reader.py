from gateway import get_gateway, get_field


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

        record._detach()
        return py_record
