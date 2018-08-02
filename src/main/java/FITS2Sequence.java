import nom.tam.fits.*;
import nom.tam.util.ArrayDataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class FITS2Sequence {

    static Logger LOG = Logger.getLogger(FITS2Sequence.class);

    public static class ColBandPartitioner extends Partitioner<Text, Text> {

        private final static Map<Character, Integer> bandMap = new HashMap<>();
        static {
            bandMap.put('u', 0);
            bandMap.put('g', 1);
            bandMap.put('r', 2);
            bandMap.put('i', 3);
            bandMap.put('z', 4);
        }

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] colband = key.toString().split("-");
            int col = Integer.parseInt(colband[0]);
            char band = colband[1].charAt(0);
            int bandId = bandMap.get(band);
            return (col - 1) * 5 + bandId;
        }
    }

    public static class NullMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ExtractReducer
            extends Reducer<Text, Text, Text, Mosaic.FloatArrayWritable> {

        private FileSystem fs;
        private CompressionCodecFactory compressionCodecFactory;
        private CompressionCodec codec;
        private Decompressor decompressor;

        private final int height = 1489, width = 2048;
        private float[] image;
        private FloatWritable[] imageWritable;

        private Text tmpKey = new Text();
        private Mosaic.FloatArrayWritable tmpValue = new Mosaic.FloatArrayWritable();

        @Override
        public void setup(Context context) throws IOException {
            image = new float[height * width];
            imageWritable = new FloatWritable[height * width];
            for (int i = 0; i < height * width; i++) {
                imageWritable[i] = new FloatWritable(0);
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            if (compressionCodecFactory == null) {
                compressionCodecFactory = new CompressionCodecFactory(conf);
            }
            String colband = key.toString();
            LOG.error(colband);
            for (Text value : values) {
                InputStream in = null;
                Path imagePath = new Path(value.toString());
                fs = imagePath.getFileSystem(conf);
                codec = compressionCodecFactory.getCodec(imagePath);

                FSDataInputStream directIn = fs.open(imagePath);
                if (codec != null) {
                    decompressor = CodecPool.getDecompressor(codec);
                    in = codec.createInputStream(directIn, decompressor);
                } else {
                    in = directIn;
                }
                try {
                    Fits fits = new Fits(in);
                    BasicHDU hdu = fits.getHDU(0);
                    Header header = hdu.getHeader();
                    double referX = header.getDoubleValue("CRPIX1");
                    double referY = header.getDoubleValue("CRPIX2");
                    double referRa = header.getDoubleValue("CRVAL1");
                    double referDec = header.getDoubleValue("CRVAL2");
                    double raDeg = header.getDoubleValue("CD1_2");
                    double decDeg = header.getDoubleValue("CD2_1");
                    double minRa = referRa - referY * raDeg;
                    double maxRa = referRa + (1489 - referY) * raDeg;
                    double minDec = referDec - referX * decDeg;
                    double maxDec = referDec + (2048 - referX) * decDeg;
//                    if (hdu.getData().reset()) {
                        ArrayDataInput adi = fits.getStream();
                        adi.read(image);
                        for (int i = 0; i < height * width; i++) {
                            imageWritable[i].set(image[i]);
                        }
                        tmpKey.set(colband + "-"
                                + minRa + "," + maxRa + ","
                                + minDec + "," + maxDec);
                        tmpValue.set(imageWritable);
                        context.write(tmpKey, tmpValue);
//                    }
                    fits.close();
                } catch (FitsException e) {
                    if (in != null) {
                        in.close();
                    }
                    throw new IOException();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FITS2Sequence");
        job.setJarByClass(FITS2Sequence.class);

        job.setMapperClass(NullMapper.class);
        job.setReducerClass(ExtractReducer.class);

        job.setNumReduceTasks(30);
        job.setPartitionerClass(ColBandPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mosaic.FloatArrayWritable.class);

        job.setInputFormatClass(FITSCombineFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job,
                DefaultCodec.class);

        FileInputFormat.addInputPath(job, inPath);
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
