package storm.bolt.Clustering.Functions;

import java.util.Map;

/**
 * Created by christina on 3/28/15.
 */
public class Distance{

    public static double computeEuclideanDistanceBetweenVectors(double[]vector1,double[]vector2){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=(vector1[i]-vector2[i])*(vector1[i]-vector2[i]);
        }
        return Math.sqrt(sum);
    }

    public static double computeManhattanDistanceBetweenVectors(double[]vector1,double[]vector2){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=Math.abs(vector1[i]-vector2[i]);
        }
        return sum;
    }

    public static double computeFuzzyDistance(double[]vector1,double[]vector2,double[]vector3,double fuzzy){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=(vector1[i]-vector2[i])*(vector1[i]-vector2[i])*Math.pow(vector3[i],fuzzy);
        }
        return sum;
    }
}
