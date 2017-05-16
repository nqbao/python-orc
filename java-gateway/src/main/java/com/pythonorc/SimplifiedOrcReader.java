package com.pythonorc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by baonguyen on 6/26/16.
 */
public class SimplifiedOrcReader implements Iterable<Object> {
    private String filePath;
    private OrcFile.ReaderOptions options;
    private Reader reader;
    private TypeDescription schema;

    public SimplifiedOrcReader(String path) {
        this.filePath = path;
    }

    public void open() throws IOException {
        this.open(new Configuration());
    }

    public void open(Configuration conf) throws IOException{
        OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
        this.reader = OrcFile.createReader(new Path(this.filePath), options);
        this.schema = this.reader.getSchema();
        this.options = options;
    }

    public void close() {
        this.reader = null;
        this.schema = null;
        this.options = null;
    }

    public List<String> getFieldNames() {
        return this.schema.getFieldNames();
    }

    public Map<String, String> getFileMetaInfo() {
        Map<String, String> dict = new HashMap<>();
        FileMetaInfo finfo = this.options.getFileMetaInfo();

        dict.put("bufferSize", String.valueOf(finfo.bufferSize));
        dict.put("metadataSize", String.valueOf(finfo.metadataSize));
        dict.put("compressionType", String.valueOf(finfo.compressionType));
        dict.put("writerVersion", String.valueOf(finfo.writerVersion.getId()));
        dict.put("versionLists", finfo.versionList.toString());

        return dict;
    }

    public long getNumberOfRows() {
        return this.reader.getNumberOfRows();
    }

    public long getNumberOfColumns() {
        return this.schema.getFieldNames().size();
    }

    public Map<String, String> getSchemaDictionary() {
        Map<String, String> dict = new HashMap<>();
        List<TypeDescription> children = this.reader.getSchema().getChildren();
        List<String> fieldNames = this.getFieldNames();

        int idx = 0;
        for (TypeDescription type : children) {
            dict.put(fieldNames.get(idx), type.getCategory().getName());
            idx++;
        }

        return dict;
    }

    @Override
    public Iterator<Object> iterator() {
        try {
            return new RecordIterator(this.reader.rows(), this.schema);
        } catch (Exception ex) {
            throw new RuntimeException("Unable to init iterator");
        }
    }

    public Iterator<Object> batch(int size) {
        try {
            return new BatchIterator(this.reader.rows(), this.schema, size);
        } catch (Exception ex) {
            throw new RuntimeException("Unable to init iterator");
        }
    }

    abstract class BaseIterator implements Iterator<Object> {
        VectorizedRowBatch batch;
        RecordReader recordReader;
        TypeDescription schema;

        int batchIndex = 0;
        long batchSize = -1;
        boolean stillAvailable = false;

        BaseIterator(RecordReader recordReader, TypeDescription schema, int size) throws IOException {
            this.recordReader = recordReader;
            this.schema = schema;
            this.batch = schema.createRowBatch(size);

            this.nextBatch();
        }

        @Override
        public boolean hasNext() {
            return this.stillAvailable && this.batchIndex < this.batchSize;
        }

        protected void nextBatch() {
            try {
                this.stillAvailable = this.recordReader.nextBatch(this.batch);
                this.batchIndex = 0;
                this.batchSize = this.batch.size;
            } catch (IOException exception) {
                throw new RuntimeException("Unable to process to next batch");
            }
        }
    }

    class BatchIterator extends BaseIterator implements Iterator<Object> {
        BatchIterator(RecordReader recordReader, TypeDescription schema, int size) throws IOException {
            super(recordReader, schema, size);
        }

        @Override
        public Object next() {
            Object[][] result = new Object[this.batch.numCols][];

            try {
                // init array map
                for (int i = 0; i < this.batch.numCols; i++) {
                    result[i] = new Object[(int)this.batchSize];
                }

                while (this.batchIndex < this.batchSize) {
                    for (int i = 0; i < this.batch.numCols; i++) {
                        Object[] batchColumn = result[i];

                        ColumnVector column = this.batch.cols[i];
                        batchColumn[this.batchIndex] = getValue(column, this.batchIndex, this.schema.getChildren().get(i));
                    }

                    this.batchIndex++;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            }

            // navigate to next batch
            if (this.batchIndex >= this.batchSize) {
                this.nextBatch();
            }

            return result;
        }
    }

    class RecordIterator extends BaseIterator {
        RecordIterator(RecordReader recordReader, TypeDescription schema) throws IOException {
            super(recordReader, schema, 1024);
        }

        @Override
        public Object next() {
            Object[] result = new Object[this.batch.numCols];

            try {
                for (int i = 0; i < this.batch.numCols; i++) {
                    ColumnVector column = this.batch.cols[i];
                    result[i] = getValue(column, this.batchIndex, this.schema.getChildren().get(i));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw ex;
            }

            this.batchIndex++;
            if (this.batchIndex >= this.batchSize) {
                this.nextBatch();
            }

            return result;
        }
    }

    private static Object getValue(ColumnVector column, int index, TypeDescription type) {
        // ref: https://orc.apache.org/docs/core-java.html
        TypeDescription.Category category = type.getCategory();

        if (column.isRepeating) {
            index = 0;
        }

        if (column.isNull[index]) {
            return null;
        } else if (column instanceof BytesColumnVector) {
            BytesColumnVector bv = (BytesColumnVector) column;

            if (bv.vector[index] != null) {
                byte[] bytes = Arrays.copyOfRange(bv.vector[index], bv.start[index], bv.start[index] + bv.length[index]);

                switch (category) {
                    case CHAR:
                    case VARCHAR:
                    case STRING:
                        return new String(bytes, StandardCharsets.UTF_8);

                    default:
                        return bytes;
                }
            }
        } else if (column instanceof LongColumnVector) {
            LongColumnVector lv = (LongColumnVector) column;
            Object value = lv.vector[index];

            switch (category) {
                case BOOLEAN:
                    value = (long)value > 0 ? true : false;
                    break;
            }

            return value;
        } else if (column instanceof DoubleColumnVector) {
            DoubleColumnVector dv = (DoubleColumnVector) column;
            return dv.vector[index];
        } else if (column instanceof ListColumnVector) {
            ListColumnVector list = (ListColumnVector) column;
            long length = list.lengths[index];
            long start = list.offsets[index];

            List<Object> items = new LinkedList<>();

            for (long i = start; i < length; i++) {
                // XXX: long to int
                items.add(getValue(list.child, (int)i, type.getChildren().get(0)));
            }

            return items.toArray();
        } else if (column instanceof StructColumnVector) {
            StructColumnVector sv = (StructColumnVector) column;

            return getStruct(sv.fields, index, type);
        } else if (column instanceof MapColumnVector) {
            MapColumnVector mv = (MapColumnVector) column;
            long length = mv.lengths[index];
            long start = mv.offsets[index];

            Dictionary<Object, Object> items = new Hashtable<>();

            for (long i = start; i < length; i++) {
                items.put(
                        getValue(mv.keys, (int) i, type.getChildren().get(0)),
                        getValue(mv.values, (int) i, type.getChildren().get(1))
                );
            }

            return items;
        } else {
            System.out.println(column.getClass().getName());
        }

        return null;
    }

    private static Object getStruct(ColumnVector[] fields, int index, TypeDescription type) {
        Dictionary<String, Object> items = new Hashtable<>();
        List<String> keys = type.getFieldNames();
        List<TypeDescription> children = type.getChildren();

        for (int i = 0; i < fields.length; i++) {
            items.put(
                    keys.get(i),
                    getValue(fields[i], index, children.get(i))
            );
        }

        return items;
    }
}
