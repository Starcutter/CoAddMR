import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

public class FITSInputFormat extends FileInputFormat<IntWritable, BytesWritable> {

    public class FITSRecordReader extends RecordReader<IntWritable, BytesWritable> {

        private CompressionCodecFactory compressionCodecFactory;
        private CompressionCodec codec;
        private Decompressor decompressor;

        private IntWritable key = new IntWritable(0);
        private BytesWritable value = new BytesWritable();

        private FileSplit split;
        private Path path;
        private FileSystem fs;
        private long start ;
        private long end;
        private long currentPos;

        private FSDataInputStream directIn;
        private InputStream in;

        private boolean processed;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext
        ) throws IOException, InterruptedException {
            Configuration conf = taskAttemptContext != null ?
                    taskAttemptContext.getConfiguration() :
                    new Configuration();
            if (compressionCodecFactory == null) {
                compressionCodecFactory = new CompressionCodecFactory(conf);
            }

            split =(FileSplit) inputSplit;
            path = split.getPath();
            fs = path.getFileSystem(conf);
            directIn = fs.open(path);

            start =split.getStart();
            end = start + split.getLength();

            codec = compressionCodecFactory.getCodec(path);
            if (codec != null) {
                decompressor = CodecPool.getDecompressor(codec);
                in = codec.createInputStream(directIn, decompressor);
            } else {
                directIn.seek(start);
                in = directIn;
            }

            processed = false;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                byte[] contents = new byte[(int) split.getLength()];
                try {
                    IOUtils.readFully(in, contents, 0, contents.length);
                    value.set(contents, 0, contents.length);
                } finally {
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public IntWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {

        }
    }

    @Override
    public RecordReader<IntWritable, BytesWritable> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        FITSRecordReader reader = new FITSRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
