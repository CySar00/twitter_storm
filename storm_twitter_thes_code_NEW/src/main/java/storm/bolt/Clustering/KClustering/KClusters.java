package storm.bolt.Clustering.KClustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.bolt.Clustering.Functions.JavaObjectsOperations;
import storm.bolt.Clustering.Functions.SerializeAndDeserializeJavaObjects;
import storm.bolt.Databases.Cassandra.KClustersDatabase;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 4/1/15.
 */
public class KClusters extends BaseRichBolt {
    OutputCollector collector;

    Map<Integer,double[]>centroidsMap=new HashMap<Integer, double[]>();
    Map<String,Integer>counts=new HashMap<String, Integer>();

    Map<String,double[]>userDataMap=new HashMap<String, double[]>();

    int[]n=new int[30];

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","USERNAME","VECTOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        getInitialCentroidsFromCassandraDB();

    }

    @Override
    public void execute(Tuple input) {
        int clusterIndex=input.getInteger(0);
        String author=input.getString(1);
        double[]vector=(double[])input.getValue(2);

        double[]resultVector=new double[vector.length];

        double[]centroidsVector=centroidsMap.get(clusterIndex);

        if(centroidsVector!=null && vector!=null){
            n[clusterIndex]+=1;
            resultVector= JavaObjectsOperations.addVectors(centroidsVector,vector,n[clusterIndex]);
            centroidsMap.put(clusterIndex,resultVector);
            updateCentroids(clusterIndex,resultVector);

            if(author!=null) {
                collector.emit(new Values(clusterIndex, author, vector));
            }
        }

//        System.out.println(clusterIndex+" "+author+" "+vector+" "+resultVector);
   //     collector.ack(input);

    }

    private void getInitialCentroidsFromCassandraDB(){
        String[] serializedCentroids= KClustersDatabase.getSerializedClusterMap();
        for(int i=0;i<serializedCentroids.length;i++){
            try{
                centroidsMap.put(i+1, SerializeAndDeserializeJavaObjects.convertStringToDoubleArray(serializedCentroids[i]));
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }

    }

    private void updateCentroids(int index,double[]vector ){
        String serializedVector=SerializeAndDeserializeJavaObjects.convertDoubleVectorToString(vector);
        KClustersDatabase.setSerializedMap(index,serializedVector);
    }


}
