package storm.bolt.Clustering.FuzzyClustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.bolt.Clustering.Functions.JavaObjectsOperations;
import storm.bolt.Clustering.Functions.SerializeAndDeserializeJavaObjects;
import storm.bolt.Databases.Cassandra.FuzzyClustersDatabase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by christina on 4/23/15.
 */
public class FuzzyClusters extends BaseRichBolt {
    OutputCollector collector;

    Map<Integer,double[]>centroids=new HashMap<Integer, double[]>();
    Map<String,Integer>counts=new HashMap<String, Integer>();
    Map<String,double[]>users=new HashMap<String, double[]>();

    int []n=new int[30];

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","AUTHOR","FEATURES"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        getInitialCentroidsFromCassandraDB();
    }

    @Override
    public void execute(Tuple input) {
        int clusterIndex=(int)input.getInteger(0);
        String author=input.getString(1);
        double[]features =(double[])input.getValue(2);

        Set<String> theAuthors=new HashSet<String>();
        Map<String,Integer>authorsAndIndexes=new HashMap<String, Integer>();

        double[]resultVector=new double[features.length];
        double[]centroidsVector=centroids.get((Integer)clusterIndex);


        if(centroidsVector!=null && features!=null) {
            n[clusterIndex]+=1;
            resultVector= JavaObjectsOperations.addVectors(centroidsVector,features,n[clusterIndex]);

            if(resultVector!=null) {
                if (!centroids.containsValue(resultVector)) {
                    centroids.put(clusterIndex, resultVector);
                    updateCentroids(clusterIndex,resultVector);
                }
            }

            if (!theAuthors.contains(author) && !authorsAndIndexes.containsKey(author)) {
                System.out.println(clusterIndex + " " + author + " " + features);
                collector.emit(new Values(clusterIndex,author,features));


                authorsAndIndexes.put(author, (Integer) clusterIndex);
                theAuthors.add(author);
            }
        }
    }

    private void getInitialCentroidsFromCassandraDB(){
        String[]serializedCentroids=FuzzyClustersDatabase.getSerializedClusterMap();
        for(int i=0;i<serializedCentroids.length;i++){
            try{
                centroids.put(i+1,SerializeAndDeserializeJavaObjects.convertStringToDoubleArray(serializedCentroids[i]));
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }

    private void updateCentroids(int clusterIndex,double[]vector){
        String serializedVectors= SerializeAndDeserializeJavaObjects.convertDoubleVectorToString(vector);
        FuzzyClustersDatabase.setSerializedMap(clusterIndex,serializedVectors);
    }
}
