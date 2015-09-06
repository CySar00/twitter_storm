package Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Classify;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class AuthorsThatHaveClusterIndexEquals1 extends BaseRichBolt {
    private OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX", "AUTHOR", "LIST"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        int index = input.getInteger(0);
        String author = input.getString(1);
        double[] vector = (double[]) input.getValue(2);

        List<Double> list = new ArrayList<Double>();
        for (int i = 0; i < vector.length; i++) {
            list.add(vector[i]);
        }

        if (index == 1) {
            collector.emit(input, new Values(index, author, list));
        }
        collector.ack(input);

    }
}