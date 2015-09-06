package Storm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.primitives.Doubles;

import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 7/21/15.
 */
public class RandomNumberGeneratorSpout extends BaseRichSpout {
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
        double []vector=new double[10];


        for(int i=0;i<vector.length;i++){
            vector[i]=10*random.nextDouble();
        }
        collector.emit(new Values(Doubles.asList(vector)));

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
