import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class Query {

    public static class RegisterMapper
            extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {

        public static Logger LOG = Logger.getLogger(RegisterMapper.class);

        @Override
        public void map(IntWritable key, BytesWritable value, Context context
        ) throws IOException, InterruptedException {
            InputStream is = null;
            try {
                is = new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
                Fits f = new Fits(is);
                // TODO core algorithm
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
            extends Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<BytesWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // TODO core algorithm
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.out.println("Argument Error!");
            System.exit(-1);
        }

        ImgFilter.readInfo();

        Path outPath = new Path(args[0]);

        double[] q = new double[4];
        for (int i = 0; i < 4; i++) {
            q[i] = Double.parseDouble(args[1 + i]);
        }
        ArrayList<ImgFilter.queryRes> infos = ImgFilter.findPicture(q);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "query");
        job.setJarByClass(Query.class);

        job.setMapperClass(RegisterMapper.class);
        job.setReducerClass(CoAddReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
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
