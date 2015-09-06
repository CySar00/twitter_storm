package storm.bolt.DataProcessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 6/21/15.
 */
public class UserAndListOfTweets extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<String>>userAndTweets=new HashMap<String, List<String>>();
    //List<String>zeTweets;

    Set<String> zeAuthors =new HashSet<String>();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","ZE_TWEETS "));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        String tweet=input.getString(1);

        List<String>zeTweets=userAndTweets.get(author);
        if(zeTweets==null){
            zeTweets=new ArrayList<String>();
        }
        zeTweets.add(author);

        System.out.println(author+" "+zeTweets);


    }
}
