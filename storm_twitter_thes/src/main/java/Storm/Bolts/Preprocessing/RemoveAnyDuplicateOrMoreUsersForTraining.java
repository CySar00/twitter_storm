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
 * Created by christina on 7/24/15.
 */
public class RemoveAnyDuplicateOrMoreUsersForTraining extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<String>>tweets;
    Set<String>unique=new HashSet<String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","TWEETS"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        tweets=new HashMap<String, List<String>>();
        unique=new HashSet<String>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        List<String> tweets1=(List<String>)input.getValue(1);

        List<String>tweets=this.tweets.get(author);
        if(tweets==null){
            tweets=new ArrayList<String>();
        }
        tweets=tweets1;
        this.tweets.put(author,tweets);

        if(!unique.contains(author)){
            collector.emit(input,new Values(author,tweets));
            unique.add(author);
        }
    }
}
