import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.ImageHDU;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class Mosaic {

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }
    }

    public static class FloatArrayWritable extends ArrayWritable {
        public FloatArrayWritable() {
            super(FloatWritable.class);
        }
    }

    @SuppressWarnings("unchecked")
    public static class FloatOrIntArrayWritable extends GenericWritable {

        private static Class<? extends Writable>[] CLASSES = null;

        static {
            CLASSES = (Class<? extends Writable>[]) new Class[]{
                    FloatArrayWritable.class,
                    IntArrayWritable.class
            };
        }

        public FloatOrIntArrayWritable() {
        }

        public FloatOrIntArrayWritable(Writable instance) {
            set(instance);
        }

        @Override
        protected Class<? extends Writable>[] getTypes() {
            return CLASSES;
        }

        @Override
        public String toString() {
            return "FloatOrIntArrayWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
        }
    }

    public static class RegisterMapper
            extends Mapper<ImgFilter.queryRes, Text, IntWritable, FloatOrIntArrayWritable> {

        static Logger LOG = Logger.getLogger(RegisterMapper.class);

        private double[] query;
        private int picHeight, picWidth;

        private Configuration conf;
        private FileSystem fs;
        private CompressionCodecFactory compressionCodecFactory;
        private CompressionCodec codec;
        private Decompressor decompressor;

        private Query[] queries;

        private IntWritable tmpKey = new IntWritable(0);
        private ArrayWritable tmpValueGray = new FloatArrayWritable();
        private ArrayWritable tmpValueCnt = new IntArrayWritable();

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            String queryStr = conf.get("query");
            queries = Query.getQueries(queryStr);
            /*
            String[] splits = queryStr.split(",");
            query = new double[4];
            for (int i = 0; i < 4; i++) {
                query[i] = Double.parseDouble(splits[i]);
            }
            */
            picHeight = SyntheticPic.standard;
//            picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));
        }

        @Override
        public void map(ImgFilter.queryRes info, Text value, Context context
        ) throws IOException, InterruptedException {
            tmpKey.set(info.queryId);
            query = queries[info.queryId].query;
            picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));

            InputStream is = null;
            Path imagePath = new Path(value.toString());
            fs = imagePath.getFileSystem(conf);
            if (compressionCodecFactory == null) {
                compressionCodecFactory = new CompressionCodecFactory(conf);
            }
            codec = compressionCodecFactory.getCodec(imagePath);

            FSDataInputStream directIn = fs.open(imagePath);
            if (codec != null) {
                decompressor = CodecPool.getDecompressor(codec);
                is = codec.createInputStream(directIn, decompressor);
            } else {
                is = directIn;
            }
            float[][] image = null;
            try {
                Fits f = new Fits(is);
                ImageHDU hdu = (ImageHDU) f.getHDU(0);
                image = (float[][]) hdu.getKernel();
                f.close();
            } catch (FitsException e) {
                LOG.error(e.getMessage());
                return;
            }

            IntWritable[] cnt = new IntWritable[picHeight * picWidth];
            for (int i = 0; i < picHeight; i++) {
                for (int j = 0; j < picWidth; j++) {
                    cnt[i * picWidth + j] = new IntWritable(0);
                }
            }
            FloatWritable[] gray = new FloatWritable[picHeight * picWidth];
            for (int i = 0; i < picHeight; i++) {
                for (int j = 0; j < picWidth; j++) {
                    gray[i * picWidth + j] = new FloatWritable(0);
                }
            }

            int h = info.pic[1] - info.pic[0];
            int w = info.pic[3] - info.pic[2];
            int height = (int)(picHeight * (info.query[1] - info.query[0]) / (query[1] - query[0]));
            int width = (int)(picWidth * (info.query[3] - info.query[2]) / (query[3] - query[2]));

            double hPropor = 1.0 * (h - 1) / height;
            double wPropor = 1.0 * (w - 1) / width;
            try {
                for (int i = 0; i < height; i++) {
                    for (int j = 0; j < width; j++) {
                        int x = i + (int) (picHeight * (info.query[0] - query[0]) / (query[1] - query[0]));
                        int y = j + (int) (picWidth * (info.query[2] - query[2]) / (query[3] - query[2]));
                        int index = x * picWidth + y;
                        if (j * wPropor + 1 >= w) {
                            if (i * hPropor + 1 >= h) {
                                gray[index].set(image[h + info.pic[0]][w + info.pic[2]]);
                            }
                            else {
                                gray[index].set(
                                        (float) (image[(int) (i * hPropor) + info.pic[0]][w + info.pic[2]] * ((int) (i * hPropor + 1) - (i * hPropor)) + image[(int) (i * hPropor) + 1 + info.pic[0]][w + info.pic[2]] * ((i * hPropor) - (int) (i * hPropor)))
                                );
                            }
                        } else {
                            if (i * hPropor + 1 >= h) {
                                gray[index].set(
                                        (float) (image[h + info.pic[0]][(int) (j * wPropor) + info.pic[2]] * ((int) (j * wPropor + 1) - (j * wPropor))
                                        + image[h + info.pic[0]][(int) (j * wPropor) + 1 + info.pic[2]] * ((j * wPropor) - (int) (j * wPropor)))
                                );
                            }
                            else {
                                gray[index].set(
                                        (float) ((float) (image[(int) (i * hPropor) + info.pic[0]][(int) (j * wPropor) + info.pic[2]] * ((int) (i * hPropor + 1) - (i * hPropor)) + image[(int) (i * hPropor) + 1 + info.pic[0]][(int) (j * wPropor) + info.pic[2]] * ((i * hPropor) - (int) (i * hPropor)))
                                        * ((int) (j * wPropor + 1) - (j * wPropor))
                                        + (float) (image[(int) (i * hPropor) + info.pic[0]][(int) (j * wPropor) + 1 + info.pic[2]] * ((int) (i * hPropor + 1) - (i * hPropor)) + image[(int) (i * hPropor) + 1 + info.pic[0]][(int) (j * wPropor) + 1 + info.pic[2]] * ((i * hPropor) - (int) (i * hPropor)))
                                        * ((j * wPropor) - (int) (j * wPropor)))
                                );
                            }
                        }
                        cnt[index].set(1);

                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {}

            tmpValueGray.set(gray);
            context.write(tmpKey, new FloatOrIntArrayWritable(tmpValueGray));
            tmpValueCnt.set(cnt);
            context.write(tmpKey, new FloatOrIntArrayWritable(tmpValueCnt));
        }
    }

    public static class CoAddReducer
            extends Reducer<IntWritable, FloatOrIntArrayWritable, IntWritable, BytesWritable> {

        static Log LOG = LogFactory.getLog(CoAddReducer.class);

        private double[] query;
        private int picHeight, picWidth;

        private Query[] queries;

        private BytesWritable tmpValueGray = new BytesWritable();

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String queryStr = conf.get("query");
            queries = Query.getQueries(queryStr);
            /*
            String[] splits = queryStr.split(",");
            query = new double[4];
            for (int i = 0; i < 4; i++) {
                query[i] = Double.parseDouble(splits[i]);
            }
            */
            picHeight = SyntheticPic.standard;
//            picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));
        }

        @Override
        public void reduce(IntWritable key, Iterable<FloatOrIntArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            query = queries[key.get()].query;
            picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));

            float[] gray = new float[picHeight * picWidth];
            int[] cnt = new int[picHeight * picWidth];

            for (FloatOrIntArrayWritable value : values) {
                Writable rawWritable = value.get();
                if (rawWritable instanceof FloatArrayWritable) {
                    FloatArrayWritable floatArrayWritable = (FloatArrayWritable) rawWritable;
                    Writable[] tmpGray = floatArrayWritable.get();
                    for (int i = 0; i < picHeight; i++) {
                        for (int j = 0; j < picWidth; j++) {
                            FloatWritable floatWritable = (FloatWritable) tmpGray[i * picWidth + j];
                            gray[i * picWidth + j] += floatWritable.get();
                        }
                    }
                } else if (rawWritable instanceof IntArrayWritable) {
                    IntArrayWritable intArrayWritable = (IntArrayWritable) rawWritable;
                    Writable[] tmpCnt = intArrayWritable.get();
                    for (int i = 0; i < picHeight; i++) {
                        for (int j = 0; j < picWidth; j++) {
                            IntWritable intWritable = (IntWritable) tmpCnt[i * picWidth + j];
                            cnt[i * picWidth + j] += intWritable.get();
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
            LOG.info("***** context.write key = " + key.get());
        }
    }

    public static void main(String[] args) throws Exception {

        /*
        if (args.length < 5) {
            System.out.println("Argument Error!");
            System.out.println("Usage: hadoop jar xx.jar Mosaic [outPath] [raMin] [raMax] [decMin] [decMax]");
            System.exit(-1);
        }

        ImgFilter.readInfo();
        */

        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        /*
        double[] q = new double[4];
        for (int i = 0; i < 4; i++) {
            q[i] = Double.parseDouble(args[1 + i]);
        }
        */

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mosaic");
        job.setJarByClass(Mosaic.class);

        job.getConfiguration().set("query", args[2]);

        /*
        job.getConfiguration().set("query", args[1] + "," + args[2] + "," + args[3] + "," + args[4]);

        ArrayList<ImgFilter.queryRes> infos = ImgFilter.findPicture(q);
        Path resFilePath = ImgFilter.writeRes2HDFS(
                infos, new Path("project/res.txt"), conf);
        job.addCacheFile(resFilePath.toUri());
        */

        job.setMapperClass(RegisterMapper.class);
        job.setReducerClass(CoAddReducer.class);

        job.setNumReduceTasks(args[1].split(";").length);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatOrIntArrayWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(FITSInputFormat.class);
        if (args.length >= 4 && args[3].toLowerCase().startsWith("fits")) {
            job.setOutputFormatClass(FITSOutputFormat.class);
        } else {
            job.setOutputFormatClass(ImageOutputFormat.class);
        }

        /*
        for (ImgFilter.queryRes info : infos) {
            FileInputFormat.addInputPath(job, new Path(info.name));
        }
        */
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
