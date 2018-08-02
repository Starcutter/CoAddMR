import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MosaicNew {

    public static class RegisterMapper
            extends Mapper<Text, Mosaic.FloatArrayWritable, IntWritable, Mosaic.FloatOrIntArrayWritable> {

        static Logger LOG = Logger.getLogger(RegisterMapper.class);

        private double[] query;
        private int picHeight, picWidth;

        private Configuration conf;

        private Query[] queries;
        private int[] picOut;
        private double[] queryOut;
        private double minRa, maxRa, minDec, maxDec;

        private final int singleHeight = 1489, singleWidth = 2048;

        private IntWritable tmpKey = new IntWritable(0);
        private ArrayWritable tmpValueGray = new Mosaic.FloatArrayWritable();
        private ArrayWritable tmpValueCnt = new Mosaic.IntArrayWritable();

        @Override
        public void setup(Context context
        ) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            String queryStr = conf.get("query");
            queries = Query.getQueries(queryStr);
            picHeight = SyntheticPic.standard;
            picOut = new int[4];
            queryOut = new double[4];
        }

        @Override
        public void map(Text key, Mosaic.FloatArrayWritable value, Context context
        ) throws IOException, InterruptedException {
            // camcol-band-rand-minRa,maxRa,minDec,macDec
            String[] splits = key.toString().split("-");
            int camcol = Integer.parseInt(splits[0]);
            char band = splits[1].charAt(0);
            String[] nums = splits[3].split(",");
            minRa = Double.parseDouble(nums[0]);
            maxRa = Double.parseDouble(nums[1]);
            minDec = Double.parseDouble(nums[2]);
            maxDec = Double.parseDouble(nums[3]);

            Writable[] image = value.get();

            for (int queryId = 0; queryId < queries.length; queryId++) {
                tmpKey.set(queryId);
                query = queries[queryId].query;
                if (!queries[queryId].isOverlapping(camcol, band)) {
                    continue;
                }

                picWidth = (int) (picHeight * (query[3] - query[2]) / (query[1] - query[0]));
                ImgFilter.queryRes info;

                if(minRa < query[1] && maxRa > query[0] &&
                        minDec < query[3] && maxDec > query[2]) {
                    if (minRa < query[0]) {
                        picOut[0] = (int) (singleHeight * (query[0] - minRa) / (maxRa - minRa));
                        queryOut[0] = query[0];
                    } else {
                        picOut[0] = 0;
                        queryOut[0] = minRa;
                    }

                    if (maxRa < query[1]) {
                        picOut[1] = singleHeight;
                        queryOut[1] = maxRa;
                    } else {
                        picOut[1] = (int) (singleHeight * (query[1] - minRa) / (maxRa - minRa));
                        queryOut[1] = query[1];
                    }

                    if (minDec < query[2]) {
                        picOut[2] = (int) (singleWidth * (query[2] - minDec) / (maxDec - minDec));
                        queryOut[2] = query[2];
                    } else {
                        picOut[2] = 0;
                        queryOut[2] = minDec;
                    }

                    if (maxDec < query[3]) {
                        picOut[3] = singleWidth;
                        queryOut[3] = maxDec;
                    } else {
                        picOut[3] = (int) (singleWidth * (query[3] - minDec) / (maxDec - minDec));
                        queryOut[3] = query[3];
                    }

                    info = new ImgFilter.queryRes("dull", picOut, queryOut, queryId);
                } else {
                    continue;
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
                                    gray[index] = (FloatWritable) image[(h + info.pic[0]) * singleWidth + w + info.pic[2]];
                                }
                                else {
                                    float tmp1 = ((FloatWritable) image[((int) (i * hPropor) + info.pic[0]) * singleWidth + w + info.pic[2]]).get();
                                    float tmp2 = ((FloatWritable) image[((int) (i * hPropor) + 1 + info.pic[0]) * singleWidth + w + info.pic[2]]).get();
                                    float res = (float) (tmp1 * ((int) (i * hPropor + 1) - (i * hPropor)) + tmp2 * ((i * hPropor) - (int) (i * hPropor)));
                                    gray[index].set(res);
                                }
                            } else {
                                if (i * hPropor + 1 >= h) {
                                    float tmp1 = ((FloatWritable) image[(h + info.pic[0]) * singleWidth + (int) (j * wPropor) + info.pic[2]]).get();
                                    float tmp2 = ((FloatWritable) image[(h + info.pic[0]) * singleWidth + (int) (j * wPropor) + 1 + info.pic[2]]).get();
                                    float res = (float) (tmp1 * ((int) (j * wPropor + 1) - (j * wPropor)) + tmp2 * ((j * wPropor) - (int) (j * wPropor)));
                                    gray[index].set(res);
                                }
                                else {
                                    float tmp1 = ((FloatWritable) image[((int) (i * hPropor) + info.pic[0]) * singleWidth + (int) (j * wPropor) + info.pic[2]]).get();
                                    float tmp2 = ((FloatWritable) image[((int) (i * hPropor) + 1 + info.pic[0]) * singleWidth + (int) (j * wPropor) + info.pic[2]]).get();
                                    float tmp3 = ((FloatWritable) image[((int) (i * hPropor) + info.pic[0]) * singleWidth + (int) (j * wPropor) + 1 + info.pic[2]]).get();
                                    float tmp4 = ((FloatWritable) image[((int) (i * hPropor) + 1 + info.pic[0]) * singleWidth + (int) (j * wPropor) + 1 + info.pic[2]]).get();
                                    gray[index].set(
                                            (float) ((float) (tmp1 * ((int) (i * hPropor + 1) - (i * hPropor)) + tmp2 * ((i * hPropor) - (int) (i * hPropor)))
                                                    * ((int) (j * wPropor + 1) - (j * wPropor))
                                                    + (float) (tmp3 * ((int) (i * hPropor + 1) - (i * hPropor)) + tmp4 * ((i * hPropor) - (int) (i * hPropor)))
                                                    * ((j * wPropor) - (int) (j * wPropor)))
                                    );
                                }
                            }
                            cnt[index].set(1);

                        }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {}

                tmpValueGray.set(gray);
                context.write(tmpKey, new Mosaic.FloatOrIntArrayWritable(tmpValueGray));
                tmpValueCnt.set(cnt);
                context.write(tmpKey, new Mosaic.FloatOrIntArrayWritable(tmpValueCnt));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MosaicNew");
        job.setJarByClass(MosaicNew.class);

        job.getConfiguration().set("query", args[2]);

        job.setMapperClass(RegisterMapper.class);
        job.setReducerClass(Mosaic.CoAddReducer.class);

        job.setNumReduceTasks(args[1].split(";").length);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Mosaic.FloatOrIntArrayWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(SequenceFITSInputFormat.class);
        job.setOutputFormatClass(ImageOutputFormat.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
