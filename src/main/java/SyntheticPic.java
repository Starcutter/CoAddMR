import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.ImageHDU;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;

import javax.imageio.ImageIO;
import javax.swing.plaf.basic.BasicInternalFrameTitlePane;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class SyntheticPic {
	final static int standard = 2048;

    private static int colorToRGB(int alpha, int red, int green, int blue) {
        int newPixel = 0;
        newPixel += alpha;
        newPixel = newPixel << 8;
        newPixel += red;
        newPixel = newPixel << 8;
        newPixel += green;
        newPixel = newPixel << 8;
        newPixel += blue;
        return newPixel;

    }
    
    public static BufferedImage synPic(double query[], ArrayList<ImgFilter.queryRes> infos) throws FitsException, IOException {

        Configuration conf = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    	int picHeight = standard;
    	int picWidth = (int) (standard * (query[3] - query[2]) / (query[1] - query[0]));
    	System.out.println("picHeightandWidth: " + picHeight + " " + picWidth);
    	
    	BufferedImage bImage = new BufferedImage(picWidth, picHeight, BufferedImage.TYPE_BYTE_GRAY);
    	int [][] cnt = new int[picHeight][picWidth];
    	float [][] gray = new float[picHeight][picWidth];
    	for(ImgFilter.queryRes info :infos) {
            System.out.println(info.toString());
    	    Path path = new Path(info.name);
            FileSystem fs = path.getFileSystem(conf);
            FSDataInputStream directIn = fs.open(path);
            CompressionCodec codec = factory.getCodec(path);
            assert codec != null;
            Decompressor decompressor = CodecPool.getDecompressor(codec);
            CompressionInputStream cIn = codec.createInputStream(directIn, decompressor);

            Fits f = new Fits(cIn);

            ImageHDU hdu = (ImageHDU) f.getHDU(0);
            float[][] image = (float[][]) hdu.getKernel();
            f.close();
            
            float [][] tempPic = new float[info.pic[1] - info.pic[0]][info.pic[3] - info.pic[2]];
            for(int i = info.pic[0]; i < info.pic[1]; i++) {
            	for(int j = info.pic[2]; j < info.pic[3]; j++) {
            		tempPic[i - info.pic[0]][j - info.pic[2]] = image[i][j];
            	}
            }
            float [][]fit = fitSize(tempPic, (int)(picHeight * (info.query[1] - info.query[0]) / (query[1] - query[0])),
            		(int)(picWidth * (info.query[3] - info.query[2]) / (query[3] - query[2])));

            for(int i = (int)(picHeight * (info.query[0] - query[0]) / (query[1] - query[0])); i < (int)(picHeight * (info.query[1] - query[0]) / (query[1] - query[0])); i++) {
                for (int j = (int) (picWidth * (info.query[2] - query[2]) / (query[3] - query[2])); j < (int) (picWidth * (info.query[3] - query[2]) / (query[3] - query[2])); j++) {
                    try {
                        gray[i][j] += (fit[i - (int) (picHeight * (info.query[0] - query[0]) / (query[1] - query[0]))][j - (int) (picWidth * (info.query[2] - query[2]) / (query[3] - query[2]))]);
                        cnt[i][j] += 1;
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                }
            }
    	}

    	float max = -10000;
    	float min = 10000;
        for(int i = 0; i < picHeight; i++) {
            for(int j = 0; j < picWidth; j++) {
                gray[i][j] /= cnt[i][j];
                if(gray[i][j] > max) max = gray[i][j];
                if(gray[i][j] <min) min = gray[i][j];
            }
        }
        for(int i = 0; i < picHeight; i++) {
            for (int j = 0; j < picWidth; j++) {
                int rgb = (int) (255 * Math.log(1000 * (gray[i][j] - min) / (max - min) + 1) / Math.log(1000));
                if(rgb > 100) System.out.println("light!! " + gray[i][j] + " " + max + " " + min + " " + rgb);
                int grayColor = colorToRGB(255, rgb, rgb, rgb);
                bImage.setRGB(j, i, grayColor);
            }
        }
    	return bImage;
    }
    
    public static float[][] fitSize(float pic[][], int height, int width) {
        int h = pic.length;
        int w = pic[0].length;

    	float [][] res = new float[height][width];
    	float [][] tempPic = new float[height][w];
    	
    	double hPropor = 1.0 * (h - 1) / height;
    	double wPropor = 1.0 * (w - 1) / width;
    	System.out.println("h and w: " + h + " " + w);
    	
    	for(int i = 0; i < height; i++) {
    		for(int j = 0; j < w; j++) {
    		    if(i * hPropor + 1 >= h)
    		        tempPic[i][j] = pic[h][j];
    		    else
    		        tempPic[i][j] = (float) (pic[(int)(i * hPropor)][j] * ((int)(i * hPropor + 1) - (i * hPropor)) + pic[(int)(i * hPropor) + 1][j] * ((i * hPropor) - (int)(i * hPropor)));
    		}
    	}
    	for(int i = 0; i < height; i++) {
    		for(int j = 0; j < width; j++) {
    		    if(j * wPropor + 1 >= w) {
                    res[i][j] = tempPic[i][w];
                }
    		    else
    		        res[i][j] = (float) (tempPic[i][(int)(j * wPropor)] * ((int)(j * wPropor + 1) - (j * wPropor))
                            + tempPic[i][(int)(j * wPropor) + 1] * ((j * wPropor) - (int)(j * wPropor)));
    		}
    	}
		return res;
    	
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length >= 4) {
            double[] q = new double[4];
            for (int i = 0; i < 4; i++) {
                q[i] = Double.parseDouble(args[i]);
            }
            ArrayList<ImgFilter.queryRes> infos = ImgFilter.main(q);
            System.out.println("Overlapping picture num = " + infos.size());
            File output = new File("i.jpg");
            ImageIO.write(synPic(q, infos), "jpg", output);
        } else {
            System.out.println("Argument Error!");
        }
    }
    
}
