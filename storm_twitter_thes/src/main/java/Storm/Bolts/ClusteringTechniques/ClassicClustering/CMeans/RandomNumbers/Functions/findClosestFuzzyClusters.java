package Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.RandomNumbers.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christina on 7/27/15.
 */
public class findClosestFuzzyClusters {

    public static double computeFuzzyDistance(double[]vector1,double[]vector2,double[]vector3,double fuzzy){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=(vector1[i]-vector2[i])*(vector1[i]-vector2[i])*Math.pow(vector3[i],fuzzy);
        }
        return sum;
    }

    public static void emit(OutputCollector collector,String []centroids,String index,double[]vector1,double []vector2,double fuzzy){
        double min=Double.MAX_VALUE;
        double diff;
        int clusterIndex=0;


        for(int i=0;i<centroids.length;i++) {

            double[]cluster=new double[vector1.length];
            List<Double>list=new ArrayList<Double>();


            String[] split = centroids[i].split(",");
            for (int j = 0; j < split.length; j++) {
                list.add(Double.valueOf(split[j]));
            }
            cluster = Doubles.toArray(list);

//            System.out.println(cluster.length + " " + vector1.length+" "+vector2.length);


            diff=computeFuzzyDistance(cluster,vector1,vector2,fuzzy);
            if(diff<min){
              min=diff;
            clusterIndex=i;
            }
        }
        collector.emit(new Values(clusterIndex,index,vector1));
    }
}
