package Storm.Bolts.ClusteringTechniques.FusionLists;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by christina on 7/30/15.
 */
public class RemoveAnyDuplicateStreams extends BaseRichBolt {
    private OutputCollector collector;
    Set<Map<String,double[]>>set;
    Map<String,double[]>map;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","MAP"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        set=new HashSet<Map<String, double[]>>();
        map=new HashMap<String, double[]>();

    }

    @Override
    public void execute(Tuple input) {
        int index=(int)input.getInteger(0);
        map=(Map<String,double[]>)input.getValue(1);


        if(map!=null) {
            if (!set.contains(map)) {
                collector.emit(input, new Values(index, map));
                set.add(map);
            }
        }
    }
}
