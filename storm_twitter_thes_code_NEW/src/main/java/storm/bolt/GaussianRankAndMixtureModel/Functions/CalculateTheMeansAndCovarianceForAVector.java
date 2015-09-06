package storm.bolt.GaussianRankAndMixtureModel.Functions;

import java.util.Arrays;

/**
 * Created by christina on 3/31/15.
 */
public class CalculateTheMeansAndCovarianceForAVector {

    public static double calculateAVectorsMeansValue(double[]vector){
        double sum=0;

        for(int i=0;i<vector.length;i++){
            sum+=vector[i];
        }
        return sum/vector.length;
    }

    public static double calculateAVectorsVariance(double[]vector){
        double sum=0;
        double means=calculateAVectorsMeansValue(vector);

        for(int i=0;i<vector.length;i++){
            sum+=(vector[i]-means)*(vector[i]-means);
        }
        return sum/vector.length;
    }

    public static double calculateAVectorsCovariance(double[]vector){
        double sum=0;

        double means=calculateAVectorsMeansValue(vector);
        for(int i=0;i<vector.length;i++){
            sum+=(vector[i]-means)*(vector[i]-means);
        }
        return  Math.sqrt(sum/vector.length);
    }

    public static double calculateAVectorsMedian(double[]vector){
        double []b=new double[vector.length];
        System.arraycopy(vector,0,b,0,b.length);
        Arrays.sort(b);

        if(vector.length%2==0) {
            return (b[(b.length / 2) - 1] + b[(b.length / 2)]) / 2;
        }else {
            return b[b.length/2];
        }
    }
}
