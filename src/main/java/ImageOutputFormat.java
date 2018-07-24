import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ImageOutputFormat extends FileOutputFormat<IntWritable, BytesWritable> {
    @Override
    public RecordWriter<IntWritable, BytesWritable> getRecordWriter(
            TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException {
        // TODO convert bytes to image
        return null;
    }
}
