package Storm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 7/31/15.
 */
public class MapWithDoublesVectorSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    Integer count = 0;
    Random random = new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        Integer index = 0;

        Map<String,double[]>map=new HashMap<String, double[]>();
        double[]features1=new double[]{27.0,2.0,8.0,5.0};
        double[]features2=new double[]{22.0,4.0,1.0,3.0};
        double[]features3=new double[]{21.0,5.0,2.0,2.0};

        map.put("a",features1);
        map.put("b",features2);
        map.put("c",features3);

        collector.emit(new Values(map));

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