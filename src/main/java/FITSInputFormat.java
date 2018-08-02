import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class FITSInputFormat extends FileInputFormat<ImgFilter.queryRes, Text> {

    static Logger LOG = Logger.getLogger(FITSInputFormat.class);

    public static class FITSRecordReader extends RecordReader<ImgFilter.queryRes, Text> {

        private CompressionCodecFactory compressionCodecFactory;
        private CompressionCodec codec;
        private Decompressor decompressor;

        private Query[] queries;

        private ImgFilter.queryRes key;
        private Text value;

        private FileSplit split;
        private Path path;
        private String fileName;
        private FileSystem fs;
        private long start;
        private long end;

        private int camcol;
        private char band;
        private double minRa, maxRa, minDec, maxDec;

        private FSDataInputStream directIn;
        private InputStream in;

        private int lastQueryId;

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
            String[] splits = fileName.split("-");
            camcol = Integer.parseInt(splits[3]);
            band = splits[1].charAt(0);
            value = new Text();
            value.set(path.toUri().toString());

            fs = path.getFileSystem(conf);
            directIn = fs.open(path);

            start = split.getStart();
            end = start + split.getLength();

            codec = compressionCodecFactory.getCodec(path);
            if (codec != null) {
                decompressor = CodecPool.getDecompressor(codec);
                in = codec.createInputStream(directIn, decompressor);
            } else {
                directIn.seek(start);
                in = directIn;
            }

            getRect(in);

            queries = Query.getQueries(conf.get("query"));
            lastQueryId = -1;
        }

        protected void getRect(InputStream in) throws IOException {
            try {
                Fits fits = new Fits(in);
                Header header = fits.getHDU(0).getHeader();
                double referX = header.getDoubleValue("CRPIX1");
                double referY = header.getDoubleValue("CRPIX2");
                double referRa = header.getDoubleValue("CRVAL1");
                double referDec = header.getDoubleValue("CRVAL2");
                double raDeg = header.getDoubleValue("CD1_2");
                double decDeg = header.getDoubleValue("CD2_1");
                minRa = referRa - referY * raDeg;
                maxRa = referRa + (1489 - referY) * raDeg;
                minDec = referDec - referX * decDeg;
                maxDec = referDec + (2048 - referX) * decDeg;
            } catch (FitsException e) {
                throw new IOException();
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (lastQueryId < queries.length - 1) {
                lastQueryId++;
                if (!queries[lastQueryId].isOverlapping(camcol, band)) {
                    continue;
                }
                double[] query = queries[lastQueryId].query;
                int[] picOut = new int[4];
                double[] queryOut = new double[4];
                int picHeight = 1489;
                int picWidth = 2048;
                if(minRa < query[1] && maxRa > query[0] &&
                        minDec < query[3] && maxDec > query[2]) {
                    if(minRa < query[0]) {
                        picOut[0] = (int) (picHeight * (query[0] - minRa) / (maxRa - minRa));
                        queryOut[0] = query[0];
                    }
                    else {
                        picOut[0] = 0;
                        queryOut[0] = minRa;
                    }

                    if(maxRa < query[1]) {
                        picOut[1] = picHeight;
                        queryOut[1] = maxRa;
                    }
                    else {
                        picOut[1] = (int) (picHeight * (query[1] - minRa) / (maxRa - minRa));
                        queryOut[1] = query[1];
                    }

                    if(minDec < query[2]) {
                        picOut[2] = (int) (picWidth * (query[2] - minDec) / (maxDec - minDec));
                        queryOut[2] = query[2];
                    }
                    else {
                        picOut[2] = 0;
                        queryOut[2] = minDec;
                    }

                    if(maxDec < query[3]) {
                        picOut[3] = picWidth;
                        queryOut[3] = maxDec;
                    }
                    else {
                        picOut[3] = (int) (picWidth * (query[3] - minDec) / (maxDec - minDec));
                        queryOut[3] = query[3];
                    }

                    key = new ImgFilter.queryRes(path.toUri().getPath(),
                            picOut, queryOut, lastQueryId);
                    return true;
                }
            }
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
            return ((float) (lastQueryId + 1)) / queries.length;
        }

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }
    }

    protected static final PathFilter NonHiddenFileFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    protected void listStatus(final FileSystem fs, Path dir, Query[] queries,
                              final List<FileStatus> result
    ) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir, NonHiddenFileFilter);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                listStatus(fs, status.getPath(), queries, result);
            } else {
                String filename = status.getPath().getName();
                String[] splits = filename.split("-");
                int camcol = Integer.parseInt(splits[3]);
                char band = splits[1].charAt(0);
                for (Query query : queries) {
                    if (query.isOverlapping(camcol, band)) {
                        result.add(status);
                        break;
                    }
                }
            }
        }
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        Configuration jobConf = job.getConfiguration();
        Query[] queries = Query.getQueries(jobConf.get("query"));
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] inputDirs = getInputPaths(job);
        for (Path dir : inputDirs) {
            FileSystem fs = dir.getFileSystem(jobConf);
            listStatus(fs, dir, queries, result);
        }
        return result;
    }

    @Override
    public RecordReader<ImgFilter.queryRes, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        return new FITSRecordReader();
    }
}
