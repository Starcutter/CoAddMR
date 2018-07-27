import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

public class ImageOutputFormat extends FileOutputFormat<IntWritable, BytesWritable> {

    public static class ImageRecordWriter extends RecordWriter<IntWritable, BytesWritable> {

        private FileSystem outFs;
        private Path outPath;

        private double[] query;
        private int picHeight, picWidth;

        public ImageRecordWriter(TaskAttemptContext taskAttemptContext, Path outPath
        ) throws IOException {
            Configuration conf = taskAttemptContext.getConfiguration();
            this.outFs = outPath.getFileSystem(conf);
            this.outPath = outPath;

            String queryStr = conf.get("query");
            String[] splits = queryStr.split(",");
            this.query = new double[4];
            for (int i = 0; i < 4; i++) {
                this.query[i] = Double.parseDouble(splits[i]);
            }
            this.picHeight = SyntheticPic.standard;
            this.picWidth = (int) (this.picHeight * (query[3] - query[2]) / (query[1] - query[0]));
        }

        @Override
        public void write(IntWritable key, BytesWritable value
        ) throws IOException, InterruptedException {
            int queryId = key.get();
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

            BufferedImage bImage = new BufferedImage(picWidth, picHeight, BufferedImage.TYPE_BYTE_GRAY);

            index = 0;
            for(int i = 0; i < picHeight; i++) {
                for (int j = 0; j < picWidth; j++) {
                    int rgb = (int) (255 * Math.log(1000 * (gray[index] - min) / (max - min) + 1) / Math.log(1000));
                    int grayColor = SyntheticPic.colorToRGB(255, rgb, rgb, rgb);
                    bImage.setRGB(j, i, grayColor);
                    index++;
                }
            }

            Path imagePath = new Path(this.outPath, "mosaic-" + queryId + ".png");
            OutputStream imageOut = this.outFs.create(imagePath);
            ImageIO.write(bImage, "png", imageOut);
            imageOut.close();
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext
        ) throws IOException, InterruptedException {

        }
    }

    @Override
    public RecordWriter<IntWritable, BytesWritable> getRecordWriter(
            TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        Path outPath = getDefaultWorkFile(taskAttemptContext, "").getParent();
        return new ImageRecordWriter(taskAttemptContext, outPath);
    }
}
