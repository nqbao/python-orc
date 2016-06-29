package com.pythonorc.gateway;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by baonguyen on 6/26/16.
 */
public class SimplifiedOrcReader implements Iterable<Object[]> {
    private String filePath;
    private Reader reader;
    private TypeDescription schema;

    public SimplifiedOrcReader(String path) {
        this.filePath = path;
    }

    public void open() throws IOException {
        this.open(new Configuration());
    }

    public void open(Configuration conf) throws IOException{
        this.reader = OrcFile.createReader(new Path(this.filePath), OrcFile.readerOptions(conf));
        this.schema = this.reader.getSchema();
    }

    public void close() {
        this.reader = null;
        this.schema = null;
    }

    public List<String> getFieldNames() {
        return this.schema.getFieldNames();
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
    public Iterator<Object[]> iterator() {
        try {
            return new RecordIterator(this.reader.rows(), this.schema.createRowBatch());
        } catch (Exception ex) {
            throw new RuntimeException("Unable to init iterator");
        }
    }

    class RecordIterator implements Iterator<Object[]> {
        VectorizedRowBatch batch;
        RecordReader recordReader;
        int batchIndex = 0;
        long batchSize = -1;
        boolean stillAvailable = false;

        RecordIterator(RecordReader recordReader, VectorizedRowBatch batch) throws IOException {
            this.recordReader = recordReader;
            this.batch = batch;

            this.nextBatch();
        }

        @Override
        public boolean hasNext() {
            return this.stillAvailable && this.batchIndex < this.batchSize;
        }

        @Override
        public Object[] next() {
            List<Object> result = new ArrayList<>();

            for (int i = 0; i < this.batch.numCols; i++) {
                ColumnVector column = this.batch.cols[i];
                result.add(this.getValue(column, this.batchIndex));
            }

            this.batchIndex++;
            if (this.batchIndex >= this.batchSize) {
                this.nextBatch();
            }

            return result.toArray();
        }

        Object getValue(ColumnVector column, int index) {
            // ref: https://orc.apache.org/docs/core-java.html
            if (column.isNull[index]) {
                return null;
            } else if (column instanceof BytesColumnVector) {
                BytesColumnVector bv = (BytesColumnVector) column;

                if (bv.vector[index] != null) {
                    byte[] bytes = Arrays.copyOfRange(bv.vector[index], bv.start[index], bv.start[index] + bv.length[index]);

                    return new String(bytes, StandardCharsets.UTF_8);
                }
            } else if (column instanceof LongColumnVector) {
                LongColumnVector lv = (LongColumnVector) column;
                return lv.vector[index];
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
                    items.add(getValue(list.child, (int)i));
                }

                return items.toArray();
            } else {
                System.out.println(column.getClass().getName());
            }

            return null;
        }

        private void nextBatch() {
            try {
                this.stillAvailable = this.recordReader.nextBatch(this.batch);
                this.batchIndex = 0;
                this.batchSize = this.batch.size;
            } catch (IOException exception) {
                throw new RuntimeException("Unable to process to next batch");
            }
        }
    }
}
