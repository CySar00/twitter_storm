package storm.bolt.Clustering.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

/**
 * Created by christina on 3/30/15.
 */
public class EmitDataIntoClosestCluster {

    public static void emitDataPointsIntoClosestKCluster(OutputCollector collector,String fuckingCentroids[],String author,double[]vector){
        double min=Double.MAX_VALUE;
        double diff;
        int index=0;

        double[]result=null;
        double[]centroidsVector;


        for(int i=0;i<fuckingCentroids.length;i++) {
            try {
                centroidsVector=SerializeAndDeserializeJavaObjects.convertStringToDoubleArray(fuckingCentroids[i]);
                diff=Distance.computeManhattanDistanceBetweenVectors(centroidsVector,vector);

                if(diff<min){
                    min=diff;
                    index=i+1;
                    if(index>1){
                        index=0;
                    }
                    result=centroidsVector;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        collector.emit(new Values(index,author,vector));
    }

  public static void emitDataPointsIntoClosestFuzzyCluster(OutputCollector collector,String[]centroids,String author,double[]vectorOfFeatures,double[]membershipVector,double fuzzyParameter){
      double min=Double.MAX_VALUE;
      double diff;
      int index=0;

      double[]result=null;
      double[] centroidsVector;

      for(int i=0;i<centroids.length;i++){
          try{
              centroidsVector=SerializeAndDeserializeJavaObjects.convertStringToDoubleArray(centroids[i]);
              diff=Distance.computeFuzzyDistance(centroidsVector,vectorOfFeatures,membershipVector,fuzzyParameter);

              if(diff<min){
                  min=diff;
                  index=i+1;
                  if(index>2){
                      index=0;
                  }
                  result=centroidsVector;
              }

          }catch (Exception ex){
              ex.printStackTrace();
          }
      }
      System.out.println(index+" "+author+" "+vectorOfFeatures);
      collector.emit(new Values(index,author,vectorOfFeatures));
  }
}
