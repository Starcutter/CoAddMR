import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SequenceFITSInputFormat
        extends SequenceFileInputFormat<Text, Mosaic.FloatArrayWritable> {

    protected static final PathFilter NonHiddenFileFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    protected void listStatus(final FileSystem fs, Path dir, Query[] queries,
                              final List<FileStatus> result
    ) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir, NonHiddenFileFilter);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                listStatus(fs, status.getPath(), queries, result);
            } else {
                String filename = status.getPath().getName();
                String[] splits = filename.split("-");
                int camcol = Integer.parseInt(splits[0]);
                char band = splits[1].charAt(0);
                for (Query query : queries) {
                    if (query.isOverlapping(camcol, band)) {
                        result.add(status);
                        break;
                    }
                }
            }
        }
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        Configuration conf = job.getConfiguration();
        Path[] inputDirs = getInputPaths(job);
        Query[] queries = Query.getQueries(conf.get("query"));
        List<FileStatus> result = new ArrayList<FileStatus>();
        for (Path dir : inputDirs) {
            FileSystem fs = dir.getFileSystem(conf);
            listStatus(fs, dir, queries, result);
        }
        return result;
    }
}
