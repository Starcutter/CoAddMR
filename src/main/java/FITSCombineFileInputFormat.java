import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class FITSCombineFileInputFormat
        extends CombineFileInputFormat<Text, Text> {

    public static class FITSCombineFileRecordReader
            extends RecordReader<Text, Text> {

        private Path path;
        private int camcol;
        private char band;
        private boolean processed;
        private Text key, value;

        public FITSCombineFileRecordReader(CombineFileSplit split,
                                           TaskAttemptContext context,
                                           Integer index
        ) throws IOException {
            this.path = split.getPath(index);
            String filename = this.path.getName();
            String[] splits = filename.split("-");
            camcol = Integer.parseInt(splits[3]);
            band = splits[1].charAt(0);
            key = new Text();
            value = new Text();
            processed = false;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext
        ) throws IOException, InterruptedException {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                key.set(camcol + "-" + band);
                value.set(this.path.toUri().toString());
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue()
                throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (processed) ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
        }
    }

    public FITSCombineFileInputFormat() {
        super();
        setMaxSplitSize(67108864);
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext
    ) throws IOException {
        return new CombineFileRecordReader<Text, Text>(
                (CombineFileSplit) inputSplit, taskAttemptContext,
                FITSCombineFileRecordReader.class
        );
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
