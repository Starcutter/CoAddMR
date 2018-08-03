import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.FitsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

public class FITSOutputFormat extends FileOutputFormat<IntWritable, BytesWritable> {

    static Logger LOG = Logger.getLogger(FITSOutputFormat.class);

    public static class FITSRecordWriter extends RecordWriter<IntWritable, BytesWritable> {

        private FileSystem outFs;
        private Path outPath;
        private Query[] queries;
        private int picHeight, picWidth;
        private boolean usePSF;

        public FITSRecordWriter(TaskAttemptContext taskAttemptContext, Path outPath
        ) throws IOException {
            Configuration conf = taskAttemptContext.getConfiguration();
            this.outFs = outPath.getFileSystem(conf);
            this.outPath = outPath;

            String queryStr = conf.get("query");
            queries = Query.getQueries(queryStr);
            this.picHeight = SyntheticPic.standard;

            if (conf.get("psf") != null) {
                usePSF = true;
            } else {
                usePSF = false;
            }
        }

        @Override
        public void write(IntWritable key, BytesWritable value) throws IOException, InterruptedException {
            int queryId = key.get();
            picWidth = (int) (this.picHeight
                    * (queries[queryId].query[3] - queries[queryId].query[2])
                    / (queries[queryId].query[1] - queries[queryId].query[0]));

            float[] gray = new float[picHeight * picWidth];
            ByteBuffer byteBuffer = ByteBuffer.wrap(value.getBytes());
            FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
            floatBuffer.get(gray);

            float max = -10000;
            float min = 10000;
            int index = 0;
            for(int i = 0; i < picHeight; i++) {
                for(int j = 0; j < picWidth; j++) {
                    if(gray[index] > max) {
                        max = gray[index];
                    }
                    if(gray[index] <min){
                        min = gray[index];
                    }
                    index++;
                }
            }

            if (usePSF) {
                gray = Deblur.deblurTest(gray, picWidth, picHeight);
            }

            float[][] gray2D = new float[picHeight][picWidth];
            for (int i = 0; i < picHeight; i++) {
                System.arraycopy(gray, (i * picWidth), gray2D[i], 0, picWidth);
            }

            Fits f = new Fits();
            try {
                f.addHDU(FitsFactory.hduFactory(gray2D));
                Path imagePath = new Path(this.outPath, "mosaic-" + queryId + ".fits");
                OutputStream imageOut = this.outFs.create(imagePath);
                DataOutputStream dos = new DataOutputStream(imageOut);
                f.write(dos);
                dos.close();
            } catch (FitsException e) {
                LOG.error(e.getMessage());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }

    @Override
    public RecordWriter<IntWritable, BytesWritable> getRecordWriter(
            TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        Path outPath = getDefaultWorkFile(taskAttemptContext, "").getParent();
        return new FITSRecordWriter(taskAttemptContext, outPath);
    }
}
