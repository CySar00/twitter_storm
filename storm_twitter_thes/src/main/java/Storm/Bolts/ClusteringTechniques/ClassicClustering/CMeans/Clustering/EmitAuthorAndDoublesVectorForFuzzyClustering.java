package Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Clustering;

import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Functions.EmitAuthorAndDoublesVectorIntoClosestFuzzyCluster;
import Storm.Databases.CassandraDB.Clustering.CCentroidsDatabase;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 7/17/15.
 */
public class EmitAuthorAndDoublesVectorForFuzzyClustering extends BaseRichBolt {
    public static final double FUZZY=1.7;

    private OutputCollector collector;
    Map<String, double[]> map;

    Random random=new Random();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX", "AUTHOR", "VECTOR"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        map = new HashMap<String, double[]>();

    }

    @Override
    public void execute(Tuple input) {
        String author = input.getString(0);
        double[] vector = (double[]) input.getValue(2);

        double[]membership=new double[vector.length];

        final String[] serializedCentroids;


        double[] mapVector = map.get(author);
        if (mapVector == null) {
            mapVector = new double[vector.length];
        }
        mapVector = vector;
        map.put(author, mapVector);

        for(int i=0;i<membership.length;i++){
            membership[i]=random.nextDouble()+0.05;
        }


        serializedCentroids = CCentroidsDatabase.getSerializedClusterMap();
        EmitAuthorAndDoublesVectorIntoClosestFuzzyCluster.emitUserIntoClosestFuzzyCluster(collector,serializedCentroids,author,vector,membership,FUZZY);


    }
}
