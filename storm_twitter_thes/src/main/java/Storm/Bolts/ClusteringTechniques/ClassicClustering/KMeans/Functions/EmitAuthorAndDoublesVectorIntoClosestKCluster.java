package Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Functions;


import backtype.storm.daemon.common.StormBase;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by christina on 7/17/15.
 */
public class EmitAuthorAndDoublesVectorIntoClosestKCluster {

    public static double calculateDistance(double[]vector1,double[] vector2){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=Math.abs(vector1[i]-vector2[i]);
        }
        return sum;
    }

    public static void emitUserIntoClosestCluster(OutputCollector collector,String[]serializedCentroids,String author,double[]vector){
        Double min=Double.MAX_VALUE;
        double diff;
        int index=0;

        double[]deser_vector;
        double[]return_vector;
        double[]result;
        double[]centroids;

        List<Double>values=new ArrayList<Double>();
        for(int i=0;i<vector.length;i++){
            values.add(vector[i]);
        }



        for(int i=0;i<serializedCentroids.length;i++){
            System.out.println(i+" "+serializedCentroids[i].length());
            centroids=new double[serializedCentroids[i].length()];
            List<Double>centroids1=new ArrayList<Double>();
            List<String>centroids2=new ArrayList<String>();

            String []temp=serializedCentroids[i].split(",");

            for(int ii=0;ii<temp.length;ii++){
                centroids2.add(temp[ii]);
            }

            for(int ii=0;ii<centroids2.size();ii++){
                centroids1.add(Double.valueOf(centroids2.get(ii)));
            }
            System.out.println(centroids1);
            centroids= Doubles.toArray(centroids1);

            System.out.println(centroids.length+" "+vector.length);
            diff=calculateDistance(centroids,vector);
            if(diff<min){
                min=diff;
                index=i;
            }
        }


        collector.emit(new Values(index,author,vector));





    }
}
