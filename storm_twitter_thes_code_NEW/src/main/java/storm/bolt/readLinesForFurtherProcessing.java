package storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by christina on 6/30/15.
 */
public class readLinesForFurtherProcessing extends BaseRichBolt{
    OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","TWEET","#TAGS","URLS","@USER","FOLLOWERS","FRIENDS","DATE","IN_REPLY_TO","TWEET_ID"));
        



    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String lineFromTextFile=input.getString(0);






    }
}
