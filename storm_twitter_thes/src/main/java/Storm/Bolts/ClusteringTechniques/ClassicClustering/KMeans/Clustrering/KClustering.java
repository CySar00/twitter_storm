package Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Clustrering;

import Storm.Databases.CassandraDB.Clustering.KCentroidsDatabase;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class KClustering extends BaseRichBolt {
    private  OutputCollector collector;

    Map<Integer,Map<String,double[]>>authors;
    Map<Integer,double[]>vectors=new HashMap<Integer, double[]>();
    Map<String,Integer>counts;

    int[] n=new int[30];


    public  void getInitialCentroidsFromKCentroidsDB(){
        String[]serializedCentroidsFromDB= KCentroidsDatabase.getSerializedClusterMap();
        for(int i=0;i<serializedCentroidsFromDB.length;i++){
            try{
                vectors.put(i,Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.deserializeJavaDoublesVector(serializedCentroidsFromDB[i]));

            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }

    public  void updateDB(int index,double[]vectorForUpdate){
        String serializedVector=Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.serializeJavaDoublesVector(vectorForUpdate);
        KCentroidsDatabase.setSerializedMap(index, serializedVector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","USERNAME","VECTOR"));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        authors=new HashMap<Integer, Map<String, double[]>>();
        getInitialCentroidsFromKCentroidsDB();

    }

    @Override
    public void execute(Tuple input) {
        String[]serialized= KCentroidsDatabase.getSerializedClusterMap();
        for(int i=0;i<serialized.length;i++){
            System.out.println(serialized[i]);
        }


        int index=(int)input.getInteger(0);
        String author=input.getString(1);
        double[]vector=(double[])input.getValue(2);

        double[]centroids=vectors.get(index);
        double[]resultVector=new double[vector.length];

        if(centroids!=null && vector!=null){
            n[index]+=1;

            for(int i=0;i<centroids.length;i++){
                resultVector[i]=(n[index]*centroids[i]+vector[i])/(n[index]+1);
            }

            vectors.put(index,resultVector);
            updateDB(index,resultVector);
        }else{
            resultVector=vector;
            vectors.put(index,resultVector);
            updateDB(index,resultVector);
        }
        System.out.println(index+" "+author+" "+vector);
        collector.emit(input,new Values(index,author,vector));
//        collector.ack(input);

    }

}
