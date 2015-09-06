package Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Clustering;

import Storm.Databases.CassandraDB.Clustering.CCentroidsDatabase;
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
public class FuzzyClustering extends BaseRichBolt {
    private OutputCollector collector;
    Map<Integer,double[]>vectors=new HashMap<Integer, double[]>();
    int[]n=new int[30];


    public void getInitialCentroidsFromFuzzyCentroidsDB(){
        String[]serilizedCentroids= CCentroidsDatabase.getSerializedClusterMap();
        for(int i=0;i<serilizedCentroids.length;i++){
            try{
                vectors.put(i,Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.deserializeJavaDoublesVector(serilizedCentroids[i]));
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }

    }

    public void updateDB(int index,double[]vectorForUpdate){
        String serializedVectors=Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.serializeJavaDoublesVector(vectorForUpdate);
        CCentroidsDatabase.setSerializedMap(index, serializedVectors);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("INDEX","USERNAME","VECTOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        getInitialCentroidsFromFuzzyCentroidsDB();
    }

    @Override
    public void execute(Tuple input) {
        int index=(int)input.getInteger(0);
        String author=input.getString(1);
        double[]vector=(double[])input.getValue(2);

        double[]centroids=vectors.get(index);
        double[]resultVector=new double[vector.length];

        if(vector!=null && centroids!=null){
            n[index]+=1;

            for(int i=0;i<centroids.length;i++){
                resultVector[i]= (n[index]*centroids[i]+vector[i])/(n[index]+1);
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
    }
}
