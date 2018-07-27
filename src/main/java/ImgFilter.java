import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ImgFilter {
	final static int picHeight = 1489;
    final static int picWidth = 2048;

    public static class picInfo {
        int run, field, camcol;
        double []coordinate = new double[4]; //raMin, raMax, decMin, decMax;

        public String toString() {
            return "301/" + run + "/" + camcol + "/frame-g-" +
                    String.format("%06d", run) + "-" + camcol + "-" + String.format("%04d", field) + ".fits.bz2";
        }

        picInfo(String init[]) {
            this.run = Integer.parseInt(init[0]);
            this.field = Integer.parseInt(init[1]);
            this.camcol = Integer.parseInt(init[2]);
            for(int i = 0; i < 4; ++i) {
                this.coordinate[i] = Double.parseDouble(init[i + 3]);
            }
        }
    }

    public static class queryRes implements Writable {
        String name;
        int []pic = new int[4];
        double []query = new double[4];

        queryRes(String name, int []pic, double []query) {
            this.name = name;
            for(int i = 0; i < 4; ++i) {
                this.pic[i] = pic[i];
                this.query[i] = query[i];
            }
        }

        queryRes(picInfo name, int []pic, double []query) {
            this.name = name.toString();
            for(int i = 0; i < 4; ++i) {
                this.pic[i] = pic[i];
                this.query[i] = query[i];
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(name + ";");
            sb.append(pic[0]);
            for(int i = 1; i < 4; i++) {
                sb.append(",").append(pic[i]);
            }
            sb.append(";").append(query[0]);
            for(int i = 1; i < 4; i++) {
                sb.append(",").append(query[i]);
            }
            sb.append("\n");
            return sb.toString();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            new Text(name).write(dataOutput);
            for (int i = 0; i < 4; i++) {
                dataOutput.writeInt(pic[i]);
            }
            for (int i = 0; i < 4; i++) {
                dataOutput.writeDouble(query[i]);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            Text tmp = new Text();
            tmp.readFields(dataInput);
            name = tmp.toString();
            for (int i = 0; i < 4; i++) {
                pic[i] = dataInput.readInt();
            }
            for (int i = 0; i < 4; i++) {
                query[i] = dataInput.readDouble();
            }
        }
    }

    public static ArrayList<picInfo> cacheCsv = new ArrayList<picInfo>();

    @SuppressWarnings("resource")
    public static void readInfo() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("field_list.csv"));
            reader.readLine(); reader.readLine();
            String line = null;
            while((line=reader.readLine()) != null){
                String item[] = line.split(",");
                cacheCsv.add(new picInfo(item));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<queryRes> findPicture(double query[]) throws ClassNotFoundException, IOException {
        ArrayList<queryRes> res = new ArrayList<queryRes>();
        int []picOut = new int[4]; double []queryOut = new double[4];
        for (picInfo pic : cacheCsv) {
            if(pic.coordinate[0] < query[1] && pic.coordinate[1] > query[0] &&
                    pic.coordinate[2] < query[3] && pic.coordinate[3] > query[2]) {
                if(pic.coordinate[0] < query[0]) {
                    picOut[0] = (int) (picHeight * (query[0] - pic.coordinate[0]) / (pic.coordinate[1] - pic.coordinate[0]));
                    queryOut[0] = query[0];
                }
                else {
                    picOut[0] = 0;
                    queryOut[0] = pic.coordinate[0];
                }

                if(pic.coordinate[1] < query[1]) {
                    picOut[1] = picHeight;
                    queryOut[1] = pic.coordinate[1];
                }
                else {
                    picOut[1] = (int) (picHeight * (query[1] - pic.coordinate[0]) / (pic.coordinate[1] - pic.coordinate[0]));
                    queryOut[1] = query[1];
                }

                if(pic.coordinate[2] < query[2]) {
                    picOut[2] = (int) (picWidth * (query[2] - pic.coordinate[2]) / (pic.coordinate[3] - pic.coordinate[2]));
                    queryOut[2] = query[2];
                }
                else {
                    picOut[2] = 0;
                    queryOut[2] = pic.coordinate[2];
                }

                if(pic.coordinate[3] < query[3]) {
                    picOut[3] = picWidth;
                    queryOut[3] = pic.coordinate[3];
                }
                else {
                    picOut[3] = (int) (picWidth * (query[3] - pic.coordinate[2]) / (pic.coordinate[3] - pic.coordinate[2]));
                    queryOut[3] = query[3];
                }

                res.add(new queryRes(pic, picOut, queryOut));
            }
        }
        return res;
    }

    public static Path writeRes2HDFS(List<queryRes> results, Path path, Configuration conf
    ) throws IOException {
        FileSystem hdfs = path.getFileSystem(conf);
        Path filePath = null;
        if (hdfs.exists(path) && hdfs.getFileStatus(path).isDirectory()) {
            filePath = new Path(path + "/res.txt");
        } else {
            filePath = path;
        }
        if (hdfs.exists(filePath)) {
            hdfs.delete(filePath, true);
        }
        FSDataOutputStream out = hdfs.create(filePath);

        for (queryRes res : results) {
            out.write(res.toString().getBytes());
        }
        out.close();

        return filePath;
    }

    public static ArrayList<queryRes> main(double args[]) throws ClassNotFoundException, IOException {
        readInfo();
        return findPicture(args);
    }
}
