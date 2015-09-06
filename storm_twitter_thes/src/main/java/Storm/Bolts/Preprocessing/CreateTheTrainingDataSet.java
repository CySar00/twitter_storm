package Storm.Bolts.Preprocessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 7/16/15.
 */
public class CreateTheTrainingDataSet extends BaseRichBolt {
    private OutputCollector collector;
    Map<String,List<String>>tweets;
    Set<String >uniqueUsers;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","TWEETS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        tweets=new HashMap<String, List<String>>();
        uniqueUsers=new HashSet<String>();
    }


    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        String tweet=input.getString(2);

        List<String>tweets=this.tweets.get(author);
        if(tweets==null){
            tweets=new ArrayList<String>();
        }
        tweets.add(tweet);
        this.tweets.put(author,tweets);

        collector.emit(input, new Values(author, tweets));


      //  collector.ack(input);


    }
}
