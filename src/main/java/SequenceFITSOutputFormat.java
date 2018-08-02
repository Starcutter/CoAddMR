import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFITSOutputFormat
        extends SequenceFileOutputFormat<Text, Mosaic.FloatArrayWritable> {

    public static class RenameOutputFormat extends FileOutputCommitter {
        private Path outPath;

        public RenameOutputFormat(Path outputPath, TaskAttemptContext context
        ) throws IOException {
            super(outputPath, context);
            this.outPath = outputPath;
        }

        @Override
        public void commitJob(JobContext context) throws IOException {
            super.commitJob(context);
            Configuration conf = context.getConfiguration();
            FileSystem outFs = outPath.getFileSystem(conf);
            FileStatus[] resultFiles = outFs.listStatus(outPath);
            if (resultFiles.length > 0) {
                for (FileStatus fileStatus : resultFiles) {
                    Path oldPath = fileStatus.getPath();
                    String oldName = oldPath.getName();
                    if (oldName.startsWith("_") || oldName.startsWith(".")) {
                        continue;
                    }
                    SequenceFile.Reader reader = new SequenceFile.Reader(
                            conf, SequenceFile.Reader.file(oldPath));
                    Text key = new Text();
                    Mosaic.FloatArrayWritable value = new Mosaic.FloatArrayWritable();
                    if (reader.next(key, value)) {
                        String[] splits = key.toString().split("-");
                        String newName = splits[0] + "-" + splits[1] + "-" + splits[2];
                        while (!outFs.rename(oldPath, new Path(oldPath.getParent(), newName)));
                    } else {
                        outFs.delete(oldPath, true);
                    }
                    reader.close();
                }
            }
        }
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter
            (TaskAttemptContext task) throws IOException {
        Path jobOutputPath = getOutputPath(task);
        return new RenameOutputFormat(jobOutputPath, task);
    }
}
