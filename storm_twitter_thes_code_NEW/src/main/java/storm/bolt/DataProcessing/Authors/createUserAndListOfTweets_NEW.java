package storm.bolt.DataProcessing.Authors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.io.Output;

import java.util.*;

/**
 * Created by christina on 6/23/15.
 */
public class createUserAndListOfTweets_NEW extends BaseRichBolt {
    OutputCollector collector;

    Map<String,List<String>>userAndTweets=new HashMap<String, List<String>>();
    Set<String>users=new HashSet<String>();
    List<String>tweets;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","ZE_TWEETS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        String aTweet=input.getString(1);


        this.tweets=userAndTweets.get(author);
        if(this.tweets==null){
            this.tweets=new ArrayList<String>();
        }

        this.tweets.add(aTweet);

        if(!users.contains(author)){
            if(!userAndTweets.containsKey(author)){
                System.out.println(author+" "+this.tweets);

                collector.emit(new Values(author,this.tweets));


                userAndTweets.put(author,this.tweets);
                users.contains(author);


            }
        }
        collector.ack(input);


    }
}
