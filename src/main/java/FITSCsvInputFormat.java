import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FITSCsvInputFormat extends FileInputFormat<ImgFilter.queryRes, Text> {

    public class FITSCsvRecordReader extends RecordReader<ImgFilter.queryRes, Text> {

        private CompressionCodecFactory compressionCodecFactory;
        private CompressionCodec codec;
        private Decompressor decompressor;

        private ImgFilter.queryRes key;
        private Text value;
//        private BytesWritable value = new BytesWritable();

        private FileSplit split;
        private Path path;
        private String fileName;
        private FileSystem fs;
        private long start ;
        private long end;

        private FSDataInputStream directIn;
        private InputStream in;
        private BufferedReader cacheBr;

        private byte[] content;

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
            fileName = path.getName();
            value = new Text();
            value.set(path.toUri().toString());
//            fs = path.getFileSystem(conf);
//            directIn = fs.open(path);
//
//            start = split.getStart();
//            end = start + split.getLength();
//
//            codec = compressionCodecFactory.getCodec(path);
//            if (codec != null) {
//                decompressor = CodecPool.getDecompressor(codec);
//                in = codec.createInputStream(directIn, decompressor);
//            } else {
//                directIn.seek(start);
//                in = directIn;
//            }
//
            processed = false;

            assert taskAttemptContext != null;
            URI resFileURI = taskAttemptContext.getCacheFiles()[0];
            String resFileName = new Path(resFileURI.getPath()).getName();
            cacheBr = new BufferedReader(new FileReader(resFileName));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
//            if (!processed) {
//                content = new byte[(int) split.getLength()];
//                try {
//                    IOUtils.readFully(in, content, 0, content.length);
//                    value.set(content, 0, content.length);
//                } finally {
//                    IOUtils.closeStream(in);
//                }
//                processed = true;
//            }

            String line = null;
            while ((line = cacheBr.readLine()) != null) {
                String[] splits = line.split(";");
                String[] segs = splits[0].split("/");
                if (segs[segs.length - 1].equals(this.fileName)) {
                    int[] pic = new int[4];
                    String[] pics = splits[1].split(",");
                    for (int i = 0; i < 4; i++) {
                        pic[i] = Integer.parseInt(pics[i]);
                    }
                    double[] query = new double[4];
                    String[] queries = splits[2].split(",");
                    for (int i = 0; i < 4; i++) {
                        query[i] = Double.parseDouble(queries[i]);
                    }
                    this.key = new ImgFilter.queryRes(splits[0], pic, query);
                    return true;
                }
            }

            processed = true;
            cacheBr.close();
            cacheBr = null;

            return false;
        }

        @Override
        public ImgFilter.queryRes getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            if (cacheBr != null) {
                cacheBr.close();
            }
        }
    }

    @Override
    public RecordReader<ImgFilter.queryRes, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        FITSCsvRecordReader reader = new FITSCsvRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
