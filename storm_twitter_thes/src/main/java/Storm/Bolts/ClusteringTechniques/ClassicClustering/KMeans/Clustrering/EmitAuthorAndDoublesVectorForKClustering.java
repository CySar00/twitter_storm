package Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Clustrering;

import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Functions.EmitAuthorAndDoublesVectorIntoClosestKCluster;
import Storm.Databases.CassandraDB.Clustering.KCentroidsDatabase;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class EmitAuthorAndDoublesVectorForKClustering extends BaseRichBolt {
    private OutputCollector collector;
    Map<String,double[]>map;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","AUTHOR","VECTOR"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        map=new HashMap<String, double[]>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        double[]vector=(double[])input.getValue(2);

        final String[]serializedCentroids;


        double[]mapVector=map.get(author);
        if(mapVector==null){
            mapVector=new double[vector.length];
        }
        mapVector=vector;
        map.put(author,mapVector);


        serializedCentroids= KCentroidsDatabase.getSerializedClusterMap();

        EmitAuthorAndDoublesVectorIntoClosestKCluster.emitUserIntoClosestCluster(collector, serializedCentroids, author, vector);



    }
}
