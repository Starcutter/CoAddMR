import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ImgFilter {
    final static int picWidth = 2048;
    final static int picHeight = 1489;

    public static class picInfo {
        int run, field, camcol;
        double []coordinate = new double[4]; //raMin, raMax, decMin, decMax;

        public String toString() {
            return "summer/301/" + run + "/" + camcol + "/frame-g-" +
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

    public static class queryRes {
        String name;
        int []pic = new int[4];
        double []query = new double[4];

        queryRes(picInfo name, int []pic, double []query) throws ClassNotFoundException, IOException {
            this.name = name.toString();
            for(int i = 0; i < 4; ++i) {
                this.pic[i] = pic[i];
                this.query[i] = query[i];
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
                    picOut[0] = (int) (picWidth * (query[0] - pic.coordinate[0]) / (pic.coordinate[1] - pic.coordinate[0]));
                    queryOut[0] = query[0];
                }
                else {
                    picOut[0] = 0;
                    queryOut[0] = pic.coordinate[0];
                }

                if(pic.coordinate[1] < query[1]) {
                    picOut[1] = picWidth;
                    queryOut[1] = pic.coordinate[1];
                }
                else {
                    picOut[1] = (int) (picWidth * (query[1] - pic.coordinate[0]) / (pic.coordinate[1] - pic.coordinate[0]));
                    queryOut[1] = query[1];
                }

                if(pic.coordinate[2] < query[2]) {
                    picOut[2] = (int) (picHeight * (query[2] - pic.coordinate[2]) / (pic.coordinate[3] - pic.coordinate[2]));
                    queryOut[2] = query[2];
                }
                else {
                    picOut[2] = 0;
                    queryOut[2] = pic.coordinate[2];
                }

                if(pic.coordinate[3] < query[3]) {
                    picOut[3] = picHeight;
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

    public static ArrayList<queryRes> main(double args[]) throws ClassNotFoundException, IOException {
        readInfo();
        return findPicture(args);
    }
}
