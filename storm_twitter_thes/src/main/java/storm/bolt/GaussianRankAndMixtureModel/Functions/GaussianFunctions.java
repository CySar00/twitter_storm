package storm.bolt.GaussianRankAndMixtureModel.Functions;

/**
 * Created by christina on 3/31/15.
 */
public class GaussianFunctions {

    public static double phi(double x){
        return Math.exp(-x*x/2)/Math.sqrt(2*Math.PI);
    }

    public static double phi(double x,double mu,double sigma){
        return phi((x-mu)/sigma)/sigma;
    }



}
