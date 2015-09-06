package Storm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.primitives.Doubles;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 7/31/15.
 */
public class ScoresMapSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    Integer count=0;
    Random random=new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        Integer index=0;

        Map<String,Map<String,Double>>scores=new HashMap<String, Map<String, Double>>();
        scores.put("Object1", new HashMap<String, Double>());
        scores.get("Object1").put("Metric 1", 1.0);
        scores.get("Object1").put("Metric 2", 1.0);
        scores.get("Object1").put("Metric 3", 0.5);
        scores.get("Object1").put("Metric 4", 0.5);

        scores.put("Object2" , new HashMap<String,Double>());
        scores.get("Object2").put("Metric 1", 0.5);
        scores.get("Object2").put("Metric 2", 0.3);
        scores.get("Object2").put("Metric 3", 0.1);
        scores.get("Object2").put("Metric 4", 0.1);

        scores.put("Object3" , new HashMap<String,Double>());
        scores.get("Object3").put("Metric 1", 0.0);
        scores.get("Object3").put("Metric 2", 0.0);
        scores.get("Object3").put("Metric 3", 0.0);
        scores.get("Object3").put("Metric 4", 0.2);

        collector.emit(new Values(scores));

    }

    @Override
    public void ack(Object msgId) {

    }
    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("vector"));
    }


}
