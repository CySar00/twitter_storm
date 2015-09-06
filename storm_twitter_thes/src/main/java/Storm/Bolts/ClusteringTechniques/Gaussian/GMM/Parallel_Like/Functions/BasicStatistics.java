package Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Parallel_Like.Functions;

import java.util.Map;

/**
 * Created by christina on 7/22/15.
 */
public class BasicStatistics {

    public static double[]calculateMUForAVector(Map<String ,double[]>map,int features){
        double []mu=new double[features];
        double[]vector=new double[map.keySet().size()];

        for(int i=0;i<features;i++){
            int count=0;

            for(String key:map.keySet()){
                vector[count]=map.get(key)[i];
                count+=1;
            }
            mu[i]=MU(vector);
        }
        return mu;
    }

    public static double MU(double[]x){
        double sum=0;

        for(int i=0;i<x.length;i++){
            sum+=x[i];
        }
        return sum/x.length;
    }

    public static double[] calculateTheSigmaForAVector(Map<String,double[]>map,int features){
        double[]sigma=new double[features];
        double[]vector=new double[map.keySet().size()];

        for(int i=0;i<features;i++){
            int count=0;
            for(String key:map.keySet()){
                vector[count]+=map.get(key)[i];
                count+=1;
            }
            sigma[i]=sigma(vector);
        }
        return sigma;
    }

    public static double sigma(double[]x){
        double sum=0;

        for(int i=0;i<x.length;i++){
            sum+=(x[i]-MU(x))*(x[i]-MU(x));
        }
        return sum/(x.length-1);
    }

    public static double[]probability(int features){
        double[]probability=new double[features];
        for(int i=0;i<probability.length;i++){
            probability[i]=1/(double)features;
        }
        return probability;
    }
}
