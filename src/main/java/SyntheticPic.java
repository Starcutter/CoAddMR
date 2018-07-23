import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.ImageHDU;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class SyntheticPic {
	final static int standard = 2048;
	final static int picWidth = 2048;
	final static int picHeight = 1489;
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
    
    public static BufferedImage synPic(double query[]) throws FitsException, IOException {
    	ArrayList<ImgFilter.queryRes> infos = new ArrayList<ImgFilter.queryRes>();
    	int picWidth = standard;
    	int picHeight = (int) (standard * (query[3] - query[2]) / (query[1] - query[0]));
    	
    	BufferedImage bImage = new BufferedImage(picWidth, picHeight, BufferedImage.TYPE_BYTE_GRAY);
    	int [][] cnt = new int[picWidth][picHeight];
    	int [][] gray = new int[picWidth][picHeight];
    	for(ImgFilter.queryRes info :infos) {

			Fits f = new Fits(info.name);
            ImageHDU hdu = (ImageHDU) f.getHDU(0);
            float[][] image = (float[][]) hdu.getKernel();
            f.close();
            
            float [][] tempPic = new float[info.pic[1] - info.pic[0]][info.pic[3] - info.pic[2]];
            for(int i = info.pic[0]; i < info.pic[1]; i++) {
            	for(int j = info.pic[2]; j < info.pic[3]; j++) {
            		tempPic[i - info.pic[0]][j - info.pic[2]] = image[i][j];
            	}
            }
            float [][]fit = fitSize(tempPic, (int)(picWidth * (info.query[1] - info.query[0]) / (query[1] - query[0])), 
            		(int)(picHeight * (info.query[3] - info.query[2]) / (query[3] - query[2])));
            
            for(int i = (int)(picHeight * info.query[2] / (query[3] - query[2])); i < (int)(picHeight * info.query[3] / (query[3] - query[2])); i++) {
            	for(int j = (int)(picWidth * info.query[0] / (query[1] - query[0])); j < (int)(picWidth * info.query[1] / (query[1] - query[0])); j++) {
            		gray[j][i] = Math.max(0, (int)(fit[i - (int)(picWidth * info.query[0] / (query[1] - query[0]))][j - (int)(picHeight * info.query[2] / (query[3] - query[2]))]));
                    cnt[j][i] += 1;
            	}
            }
            for(int i = 0; i < picHeight; i++) {
            	for(int j = 0; j < picWidth; j++) {
            		int grayColor = colorToRGB(255, gray[j][i] / cnt[j][i], gray[j][i] / cnt[j][i], gray[j][i] / cnt[j][i]);
                    bImage.setRGB(j, i, grayColor);
            	}
            }
    	}
    	return bImage;
    }
    
    public static float[][] fitSize(float pic[][], int width, int height) {
    	float [][] res = new float[width][height];
    	int w = pic.length;
    	int h = pic[0].length;
    	double wPropor = 1.0 * width / w;
    	double hPropor = 1.0 * height / h;
    	for(int i = 0; i < width; i++) {
    		for(int j = 0; j < h; j++) {
    			if((i * wPropor) - (int)(i * wPropor) < 1e-5)
    				res[i][j] = pic[(int)(i * wPropor)][j];
    			else if((int)(i * wPropor + 1) - (i * wPropor) < 1e-5)
    				res[i][j] = pic[(int)(i * wPropor + 1)][j];
    			else
    				res[i][j] = (float) (pic[(int)(i * wPropor)][j] * ((i * wPropor) - (int)(i * wPropor)) + 
    					pic[(int)(i * wPropor) + 1][j] * ((int)(i * wPropor + 1) - (i * wPropor)));
    		}
    	}
    	for(int i = 0; i < width; i++) {
    		for(int j = 0; j < height; j++) {
    			if((j * hPropor) - (int)(j * hPropor) < 1e-5)
    				res[i][j] = pic[i][(int)(j * hPropor)];
    			else if((int)(j * hPropor + 1) - (j * hPropor) < 1e-5)
    				res[i][j] = pic[i][(int)(j * hPropor + 1)];
    			res[i][j] = (float) (pic[i][(int)(j * hPropor)] * ((j * hPropor) - (int)(j * hPropor)) + 
    					pic[i][(int)(j * hPropor) + 1] * ((int)(j * hPropor + 1) - (j * hPropor)));
    		}
    	}
		return res;
    	
    }
    
    public static void main(String[] args) throws Exception {
    	double []q = new double[4];
        File output = new File("i.jpg");
        ImageIO.write(synPic(q), "jpg", output);
    }
    
    public final double[][] initCoefficients(double[][] c) 
    {
       final int N = c.length;
       final double value = 1/Math.sqrt(2.0);

       for (int i=1; i<N; i++) 
       {
           for (int j=1; j<N; j++) 
           {
               c[i][j]=1;
           }
       }

       for (int i=0; i<N; i++) 
       {
           c[i][0] = value;
           c[0][i] = value;
       }
       c[0][0] = 0.5;
       return c;
   }

   /* Computes the discrete cosine transform
    */
   public final double[][] forwardDCT(double[][] input) 
   {
       final int N = input.length;
       final double mathPI = Math.PI;
       final int halfN = N/2;
       final double doubN = 2.0*N;

       double[][] c = new double[N][N];
       c = initCoefficients(c);

       double[][] output = new double[N][N];

       for (int u=0; u<N; u++) 
       {
           double temp_u = u*mathPI;
           for (int v=0; v<N; v++) 
           {
               double temp_v = v*mathPI;
               double sum = 0.0;
               for (int x=0; x<N; x++) 
               {
                   int temp_x = 2*x+1;
                   for (int y=0; y<N; y++) 
                   {
                       sum += input[x][y] * Math.cos((temp_x/doubN)*temp_u) * Math.cos(((2*y+1)/doubN)*temp_v);
                   }
               }
               sum *= c[u][v]/ halfN;
               output[u][v] = sum;
           }
       }
       return output;
   }

   /* 
    * Computes the inverse discrete cosine transform
    */
   public final double[][] inverseDCT(double[][] input) 
   {
       final int N = input.length;
       final double mathPI = Math.PI;
       final int halfN = N/2;
       final double doubN = 2.0*N;

       double[][] c = new double[N][N];
       c = initCoefficients(c);

       double[][] output = new double[N][N];


       for (int x=0; x<N; x++) 
       {
           int temp_x = 2*x+1;
           for (int y=0; y<N; y++) 
           {
               int temp_y = 2*y+1;
               double sum = 0.0;
               for (int u=0; u<N; u++) 
               {
                   double temp_u = u*mathPI;
                   for (int v=0; v<N; v++) 
                   {
                       sum += c[u][v] * input[u][v] * Math.cos((temp_x/doubN)*temp_u) * Math.cos((temp_y/doubN)*v*mathPI);
                   }
               }
               sum /= halfN;
               output[x][y] = sum;
           }
      }
      return output;
   }
}
