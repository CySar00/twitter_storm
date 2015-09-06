package storm.bolt.Clustering.KClustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import storm.bolt.Clustering.Functions.EmitDataIntoClosestCluster;
import storm.bolt.Databases.Cassandra.KClustersDatabase;

import java.util.*;

/**
 * Created by christina on 4/1/15.
 */
public class EmitDataPoints extends BaseRichBolt {
    OutputCollector collector;
    Map<Integer,String>centroids=new HashMap<Integer, String>();

    String[]serialized;

    Set<String>theAuthors=new HashSet<String>();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","USERNAME","VECTOR"));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        double[]vector=(double[])input.getValue(1);

        serialized=KClustersDatabase.getSerializedClusterMap();

        if(!theAuthors.contains(author)) {
            EmitDataIntoClosestCluster.emitDataPointsIntoClosestKCluster(collector, serialized, author, vector);
            theAuthors.add(author);
        }

    }
}

