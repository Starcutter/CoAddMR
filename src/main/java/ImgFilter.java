import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ImgFilter {
	final static int picHeight = 1489;
    final static int picWidth = 2048;

    public static class picInfo {
        int run, field, camcol;
        String band;
        double []coordinate = new double[4]; //raMin, raMax, decMin, decMax;

        public String toString() {
            return "301/" + run + "/" + camcol + "/frame-" + band + "-" +
                    String.format("%06d", run) + "-" + camcol + "-" + String.format("%04d", field) + ".fits.bz2";
        }

        picInfo(String init[], String band) {
            this.run = Integer.parseInt(init[0]);
            this.field = Integer.parseInt(init[1]);
            this.camcol = Integer.parseInt(init[2]);
            this.band = band;
            for(int i = 0; i < 4; ++i) {
                this.coordinate[i] = Double.parseDouble(init[i + 3]);
            }
        }
    }

    public static class queryRes implements Writable {
        String name;
        int []pic = new int[4];
        double []query = new double[4];
        int queryId;

        queryRes(String name, int []pic, double []query, int queryId) {
            this(name, pic, query);
            this.queryId = queryId;
        }

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

        public void setName(String name) {
            this.name = name;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(queryId + ";");
            sb.append(name).append(";");
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
            dataOutput.writeInt(queryId);
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
            queryId = dataInput.readInt();
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
                cacheCsv.add(new picInfo(item, "g"));
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

    public static queryRes queryOverlapping(double[] query , InputStream in) {
        try {
            byte[] tempbytes = new byte[80];
            int byteread = 0;
            int num = 1;
            while(num < 66) {
                byteread = in.read(tempbytes);
                num++;
            }
            byte[] crpix1 = new byte[80];
            byte[] crpix2 = new byte[80];
            byte[] crval1 = new byte[80];
            byte[] crval2 = new byte[80];
            byte[] cd1 = new byte[80];
            byte[] cd2 = new byte[80];
            byteread = in.read(crpix1);
            byteread = in.read(crpix2);
            byteread = in.read(crval1);
            byteread = in.read(crval2);
            byteread = in.read(tempbytes);
            byteread = in.read(cd1);
            byteread = in.read(cd2);
            double referX = asciiToDouble(crpix1);
            double referY = asciiToDouble(crpix2);
            double referRa = asciiToDouble(crval1);
            double referDec = asciiToDouble(crval2);
            double raDeg = asciiToDouble(cd1);
            double decDeg = asciiToDouble(cd2);
            double minRa = referRa - referY * raDeg;
            double maxRa = referRa + (1489 - referY) * raDeg;
            double minDec = referDec - referX * decDeg;
            double maxDec = referDec + (2048 - referX) * decDeg;
            //System.out.println(minRa + "," + maxRa + "," + minDec + "," + maxDec);
            int[] picOut = new int[4];
            double[] queryOut = new double[4];
            int picHeight = 1489;
            int picWidth = 2048;
            if(minRa < query[1] && maxRa > query[0] &&
                    minDec < query[3] && maxDec > query[2]) {
                if(minRa < query[0]) {
                    picOut[0] = (int) (picHeight * (query[0] - minRa) / (maxRa - minRa));
                    queryOut[0] = query[0];
                }
                else {
                    picOut[0] = 0;
                    queryOut[0] = minRa;
                }

                if(maxRa < query[1]) {
                    picOut[1] = picHeight;
                    queryOut[1] = maxRa;
                }
                else {
                    picOut[1] = (int) (picHeight * (query[1] - minRa) / (maxRa - minRa));
                    queryOut[1] = query[1];
                }

                if(minDec < query[2]) {
                    picOut[2] = (int) (picWidth * (query[2] - minDec) / (maxDec - minDec));
                    queryOut[2] = query[2];
                }
                else {
                    picOut[2] = 0;
                    queryOut[2] = minDec;
                }

                if(maxDec < query[3]) {
                    picOut[3] = picWidth;
                    queryOut[3] = maxDec;
                }
                else {
                    picOut[3] = (int) (picWidth * (query[3] - minDec) / (maxDec - minDec));
                    queryOut[3] = query[3];
                }

                return new queryRes("zhj", picOut, queryOut);
            }
        } catch(Exception e1) {
            e1.printStackTrace();
        }
        return null;
    }

    public static Double asciiToDouble(byte[] source) {
        StringBuffer tmp = new StringBuffer();
        for(int i = 0; i < 80; i++) {
            char temp = (char) source[i];
            tmp.append(temp);
        }
        String tempString = tmp.toString();
        int pos1 = tempString.indexOf("=");
        int pos2 = tempString.indexOf("/");
        String dst = tempString.substring(pos1 + 1, pos2);
        double result = Double.parseDouble(dst);

        return result;
    }

    public static CompressionCodecFactory factory = null;

    public static ArrayList<queryRes> filter(double query[], String band,
                                             Path path, FileSystem fs
                                             ) throws IOException {
        ArrayList<queryRes> res = new ArrayList<queryRes>();
        FileStatus[] files = fs.listStatus(path);
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                res.addAll(filter(query, band, file.getPath(), fs));
            } else if (file.isFile()) {
                String filename = file.getPath().getName();
                if (filename.split("-")[1].equals(band)) {
                    FSDataInputStream directIn = fs.open(file.getPath());
                    if (factory == null) {
                        factory = new CompressionCodecFactory(fs.getConf());
                    }
                    CompressionCodec codec = factory.getCodec(file.getPath());
                    Decompressor decompressor = CodecPool.getDecompressor(codec);
                    CompressionInputStream cIn = codec.createInputStream(directIn, decompressor);
                    queryRes singleRes = queryOverlapping(query, cIn);
                    if (singleRes != null) {
                        singleRes.setName(file.getPath().toUri().getPath());
                        res.add(singleRes);
                    }
                }
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
