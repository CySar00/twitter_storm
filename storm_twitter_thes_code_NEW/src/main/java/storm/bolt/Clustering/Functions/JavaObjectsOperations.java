package storm.bolt.Clustering.Functions;

/**
 * Created by christina on 4/1/15.
 */
public class JavaObjectsOperations {

    public static double[] addVectors(double[]vector1,double[]vector2,int n){
        double vector3[]=new double[vector1.length];

        for(int i=0;i<vector1.length;i++){
            vector3[i]=(n*vector2[i]+vector1[i])/(n+1);
        }
        return vector3;
    }
}
