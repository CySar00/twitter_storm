package Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christina on 7/17/15.
 */
public class EmitAuthorAndDoublesVectorIntoClosestFuzzyCluster {

    public static double computeFuzzyDistance(double[]vector1,double []vector2,double vector3[],double fuzzy){

        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=(vector1[i]-vector2[i])*(vector1[i]-vector2[i])*Math.pow(vector3[i],fuzzy);
        }
        return sum;
    }

    public static void emitUserIntoClosestFuzzyCluster(OutputCollector collector,String[]serializedCentroids,String author,double[]vector1,double[] vector2,double fuzzy){
        double min=Double.MAX_VALUE;
        double diff;
        int index=0;

        double[]deser_vector;
        double[]return_vector;
        double[]result;

        for(int i=0;i<serializedCentroids.length;i++){
            System.out.println(i+" "+serializedCentroids[i].length());
            double[]centroids;
            centroids=new double[serializedCentroids[i].length()];
            List<Double>centroids1=new ArrayList<Double>();

            String []temp=serializedCentroids[i].split(",");

            for(int ii=0;ii<temp.length;ii++){
                centroids1.add(Double.valueOf(temp[ii]));
            }
            centroids=Doubles.toArray(centroids1);

            System.out.println(centroids.length+" "+vector1.length);
            diff=computeFuzzyDistance(centroids, vector1, vector2, fuzzy);
            if(diff<min){
                min=diff;
                index=i;
            }
        }

        //System.out.println(index+" "+author+" "+vector1);
        collector.emit(new Values(index,author,vector1));

    }
}
