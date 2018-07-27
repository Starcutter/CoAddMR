import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.ImageHDU;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class Query {

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }
    }

    @SuppressWarnings("unchecked")
    public static class BytesOrIntArrayWritable extends GenericWritable {

        private static Class<? extends Writable>[] CLASSES = null;

        static {
            CLASSES = (Class<? extends Writable>[]) new Class[]{
                    BytesWritable.class,
                    IntArrayWritable.class
            };
        }

        public BytesOrIntArrayWritable() {
        }

        public BytesOrIntArrayWritable(Writable instance) {
            set(instance);
        }

        @Override
        protected Class<? extends Writable>[] getTypes() {
            return CLASSES;
        }

        @Override
        public String toString() {
            return "BytesOrIntArrayWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
        }
    }

    public static class RegisterMapper
            extends Mapper<ImgFilter.queryRes, BytesWritable, IntWritable, BytesOrIntArrayWritable> {

        static Logger LOG = Logger.getLogger(RegisterMapper.class);

        private double[] query;
        private int picHeight, picWidth;

        private IntWritable tmpKey = new IntWritable(0);
        private BytesWritable tmpValueGray = new BytesWritable();
        private IntArrayWritable tmpValueCnt = new IntArrayWritable();

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String queryStr = conf.get("query");
            String[] splits = queryStr.split(",");
            query = new double[4];
            for (int i = 0; i < 4; i++) {
                query[i] = Double.parseDouble(splits[i]);
            }
            picHeight = SyntheticPic.standard;
            picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));
        }

        @Override
        public void map(ImgFilter.queryRes key, BytesWritable value, Context context
        ) throws IOException, InterruptedException {
            InputStream is = null;
            try {
                is = new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
                Fits f = new Fits(is);
                ImageHDU hdu = (ImageHDU) f.getHDU(0);
                float[][] image = (float[][]) hdu.getKernel();
                f.close();

                ImgFilter.queryRes info = key;
                float [][] tempPic = new float[info.pic[1] - info.pic[0]][info.pic[3] - info.pic[2]];
                for(int i = info.pic[0]; i < info.pic[1]; i++) {
                    for (int j = info.pic[2]; j < info.pic[3]; j++) {
                        tempPic[i - info.pic[0]][j - info.pic[2]] = image[i][j];
                    }
                }

                IntWritable[] cnt = new IntWritable[picHeight * picWidth];
                float[] gray = new float[picHeight * picWidth];

                float[][] fit = SyntheticPic.fitSize(tempPic,
                        (int)(picHeight * (info.query[1] - info.query[0]) / (query[1] - query[0])),
                        (int)(picWidth * (info.query[3] - info.query[2]) / (query[3] - query[2])));

                for (int i = (int)(picHeight * (info.query[0] - query[0]) / (query[1] - query[0]));
                    i < (int)(picHeight * (info.query[1] - query[0]) / (query[1] - query[0])); i++) {
                    for (int j = (int) (picWidth * (info.query[2] - query[2]) / (query[3] - query[2]));
                         j < (int) (picWidth * (info.query[3] - query[2]) / (query[3] - query[2])); j++) {
                        try {
                            gray[i * picWidth + j] = (fit[i - (int) (picHeight * (info.query[0] - query[0]) / (query[1] - query[0]))][j - (int) (picWidth * (info.query[2] - query[2]) / (query[3] - query[2]))]);
                            cnt[i * picWidth + j].set(cnt[i * picWidth].get() + 1);
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }
                    }
                }

                // convert float[][] gray to BytesWritable and int[][] cnt to IntArrayWritable
                byte[] byteArray = new byte[picHeight * picWidth * 4];
                ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
                FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
                floatBuffer.put(gray);

                tmpValueGray.set(byteArray, 0, byteArray.length);
                context.write(tmpKey, new BytesOrIntArrayWritable(tmpValueGray));

                tmpValueCnt.set(cnt);
                context.write(tmpKey, new BytesOrIntArrayWritable(tmpValueCnt));

            } catch (FitsException e) {
                LOG.error(e.getMessage());
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }
    }

    public static class CoAddReducer
            extends Reducer<IntWritable, BytesOrIntArrayWritable, IntWritable, BytesWritable> {

        private double[] query;
        private int picHeight, picWidth;

        private BytesWritable tmpValueGray = new BytesWritable();

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String queryStr = conf.get("query");
            String[] splits = queryStr.split(",");
            query = new double[4];
            for (int i = 0; i < 4; i++) {
                query[i] = Double.parseDouble(splits[i]);
            }
            picHeight = SyntheticPic.standard;
            picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));
        }

        @Override
        public void reduce(IntWritable key, Iterable<BytesOrIntArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            float[] gray = new float[picHeight * picWidth];
            int[] cnt = new int[picHeight * picWidth];

            for (BytesOrIntArrayWritable value : values) {
                Writable rawWritable = value.get();
                if (rawWritable instanceof BytesWritable) {
                    BytesWritable bytesWritable = (BytesWritable) rawWritable;
                    float[] tmpGray = new float[bytesWritable.getLength() / 4];
                    ByteBuffer byteBuffer = ByteBuffer.wrap(bytesWritable.getBytes());
                    FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
                    floatBuffer.get(tmpGray);

                    for (int i = 0; i < picHeight; i++) {
                        for (int j = 0; j < picWidth; j++) {
                            gray[i * picWidth + j] += tmpGray[i * picWidth + j];
                        }
                    }
                } else if (rawWritable instanceof IntArrayWritable) {
                    Writable[] tmpCnt = ((IntArrayWritable) rawWritable).get();
                    for (int i = 0; i < picHeight; i++) {
                        for (int j = 0; j < picWidth; j++) {
                            cnt[i * picWidth + j] += ((IntWritable) tmpCnt[i * picWidth + j]).get();
                        }
                    }
                }
            }

            for(int i = 0; i < picHeight; i++) {
                for (int j = 0; j < picWidth; j++) {
                    gray[i * picWidth + j] /= cnt[i * picWidth + j];
                }
            }
            byte[] byteArray = new byte[picHeight * picWidth * 4];
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
            FloatBuffer floatBuffer = byteBuffer.asFloatBuffer();
            floatBuffer.put(gray);

            tmpValueGray.set(byteArray, 0, byteArray.length);
            context.write(key, tmpValueGray);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.out.println("Argument Error!");
            System.out.println("Usage: hadoop jar xx.jar Query [outPath] [raMin] [raMax] [decMin] [decMax]");
            System.exit(-1);
        }

        ImgFilter.readInfo();

        Path outPath = new Path(args[0]);

        double[] q = new double[4];
        for (int i = 0; i < 4; i++) {
            q[i] = Double.parseDouble(args[1 + i]);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query");
        job.setJarByClass(Query.class);

        conf.set("query", args[1] + "," + args[2] + "," + args[3] + "," + args[4]);
        conf.set("picHeight", String.valueOf(SyntheticPic.standard));
        int picWidth = (int) (SyntheticPic.standard * (q[3] - q[2]) / (q[1] - q[0]));
        conf.set("picWidth", String.valueOf(picWidth));

        ArrayList<ImgFilter.queryRes> infos = ImgFilter.findPicture(q);
        Path resFilePath = ImgFilter.writeRes2HDFS(
                infos, new Path("project/res.txt"), conf);
        job.addCacheFile(resFilePath.toUri());

        job.setMapperClass(RegisterMapper.class);
        job.setReducerClass(CoAddReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesOrIntArrayWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(FITSInputFormat.class);
        job.setOutputFormatClass(ImageOutputFormat.class);

        for (ImgFilter.queryRes info : infos) {
            FileInputFormat.addInputPath(job, new Path(info.name));
        }
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
