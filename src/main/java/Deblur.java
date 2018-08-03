import edu.emory.mathcs.jtransforms.fft.DoubleFFT_2D;
import org.apache.commons.math3.analysis.function.Sinc;
import org.apache.commons.math3.complex.Complex;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;

public class Deblur {

    private static final double T = 1.0;
    private static final double A = 0.1;
    private static final double B = 0.1;

    public static void fftForward(float[] img,
                                  int width, int height, double[] fft) {
        DoubleFFT_2D fftDo = new DoubleFFT_2D(width, height);
        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {
                double r = img[x * width + y];
                fft[2*(x + y*width)] = r;
                fft[2*(x + y*width) + 1] = 0;
            }
        }
        fftDo.complexForward(fft);
    }

    public static void fftInverse(double[] fft, double[] out, int width, int height) {
        DoubleFFT_2D fftDo = new DoubleFFT_2D(width, height);
        fftDo.complexInverse(fft, true);

        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {
                double real = fft[y*2*width + 2*x];
                out[y*width + x] = real;
            }
        }
    }

    public static void normalizeToMaxAbsValue(Complex[] data, int width, int height, double value)
    {
        double max = Double.MIN_VALUE;

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                Complex c = data[ x + y*width ];
                double abs = Math.sqrt(c.getReal() * c.getReal() + c.getImaginary() * c.getImaginary() );
                max = Math.max( max, abs );
            }
        }

        for ( int x = 0; x < data.length; x++ ) {
            data[x] = data[x].multiply( value / max );
        }
    }

    private static Complex[] motionBlur(double[] degradation, int width, int height) {
        Complex[] complex = new Complex[width * height];

        double[] temp = new double[2 * width * height];

        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {

                double teta = Math.PI*((x - width/2)%width*A + (y - height/2)%height*B);

                Sinc sinc = new Sinc();

                double real = Math.cos(teta) * sinc.value(teta) * T;
                double imaginary = Math.sin(teta) * sinc.value(teta) * T;

                Complex c = new Complex(real, imaginary);
                Complex cConj = c.conjugate();

                temp[2*(x + y*width)] = cConj.getReal();
                temp[2*(x + y*width) + 1] = cConj.getImaginary();
            }
        }

        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {
                int xTranslated = (x + width/2) % width;
                int yTranslated = (y + height/2) % height;

                double real = temp[2*(yTranslated*width + xTranslated)];
                double imaginary = temp[2*(yTranslated*width + xTranslated) + 1];

                degradation[2*(x + y*width)] = real;
                degradation[2*(x + y*width) + 1] = imaginary;

                Complex c = new Complex(real, imaginary);
                complex[y*width + x] = c;
            }
        }

        return complex;
    }

    private static Complex deconvolutionByWiener(Complex imagem, Complex psf) {
        double K = Math.pow(1.07, 32)/10000.0;
        double energyValue = Math.pow(psf.getReal(), 2) + Math.pow(psf.getImaginary(), 2);
        double wienerValue = energyValue / (energyValue + K);

        Complex divided = imagem.divide(psf);
        Complex c = divided.multiply(wienerValue);

        return c;
    }

    public static void globalExpansion2(double[] magnitude, float[] pixels, int width, int height) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;

        int x_y;
        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {
                x_y = x + y*width;

                if(magnitude[x_y] < min) min = magnitude[x_y];
                if(magnitude[x_y] > max) max = magnitude[x_y];
            }
        }

        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {
                x_y = x + y*width;

                int lum;

                lum = (int)(255.0*((magnitude[x_y])-min) / (max-min) + 0.5);

                if(lum < 0) lum = 0;
                else if(lum > 255) lum = 255;

                pixels[x_y] = 0xFF000000 | lum << 16 | lum << 8 | lum;
            }
        }
    }

    public static float[] deblurTest(float[] img, int width, int height) throws IOException {

        double[] fft = new double[2 * width * height];
        fftForward(img, width, height, fft);

        Complex[] complexImage = new Complex[width * height];

        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {

                Complex c = new Complex(fft[2*(x + y*width)], fft[2*(x + y*width) + 1]);
                complexImage[x + y*width] = c;

            }
        }

        normalizeToMaxAbsValue(complexImage, width, height, 1);

        double[] degradation = new double[2*width * height];

        Complex[] complexPsf = motionBlur(degradation, width, height);

        normalizeToMaxAbsValue(complexPsf, width, height, 1);

        double[] convoluted = new double[2 * width * height];
        for(int y = 0; y < height; y++) {
            for(int x = 0; x < width; x++) {
                Complex complexImg = complexImage[x + y*width];

                Complex complexDeg = complexPsf[x + y*width];

                Complex m = deconvolutionByWiener(complexImg, complexDeg);

                convoluted[2*(x + y*width)] = m.getReal();
                convoluted[2*(x + y*width) + 1] = m.getImaginary();
            }
        }

        double[] imageResult = new double[width * height];
        fftInverse(convoluted, imageResult, width, height);

        float[] image = new float[imageResult.length];
        globalExpansion2(imageResult, image, width, height);

        return image;
    }
}
