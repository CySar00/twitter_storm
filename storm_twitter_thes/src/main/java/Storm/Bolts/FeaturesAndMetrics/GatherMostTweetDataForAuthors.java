package Storm.Bolts.FeaturesAndMetrics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 7/26/15.
 */
public class GatherMostTweetDataForAuthors extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<Long>>IDs;
    Map<String,List<String>>tweets;
    Map<String,List<Long>>inReplyTos;
    Map<String,List<Date>>dates;
    Map<String,List<String>>followers;
    Map<String,List<String>>friends;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","IDS","TWEETS","DATES","IN_REPLY_TOS","FOLLOWERS","FRIENDS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        IDs=new HashMap<String, List<Long>>();
        tweets=new HashMap<String, List<String>>();
        inReplyTos=new HashMap<String, List<Long>>();
        dates=new HashMap<String, List<Date>>();

        followers=new HashMap<String, List<String>>();
        friends=new HashMap<String, List<String>>();


    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        Long ID=input.getLong(1);
        String tweet=input.getString(2);
        Date date=(Date)input.getValue(3);
        Long inReplyTo=input.getLong(4);

        List<String>followers1=(List<String>)input.getValue(5);
        List<String>friends1=(List<String>)input.getValue(6);

        List<Long>IDs=this.IDs.get(author);
        if(IDs==null){
            IDs=new ArrayList<Long>();
        }
        IDs.add(ID);
        this.IDs.put(author,IDs);

        List<String>tweets=this.tweets.get(author);
        if(tweets==null){
            tweets=new ArrayList<String>();
        }
        tweets.add(tweet);
        this.tweets.put(author,tweets);

        List<Long>inReplyTos=this.inReplyTos.get(author);
        if(inReplyTos==null){
            inReplyTos=new ArrayList<Long>();
        }
        inReplyTos.add(inReplyTo);
        this.inReplyTos.put(author,inReplyTos);

        List<Date>dates=this.dates.get(author);
        if(dates==null){
            dates=new ArrayList<Date>();
        }
        dates.add(date);
        this.dates.put(author,dates);

        List<String>followers=this.followers.get(author);
        if(followers==null){
            followers=new ArrayList<String>();
        }
        followers=followers1;
        this.followers.put(author,followers);

        List<String>friends=this.friends.get(author);
        if(friends==null){
            friends=new ArrayList<String>();
        }
        friends=friends1;
        this.friends.put(author,friends);




        collector.emit(input,new Values(author,IDs,tweets,dates,inReplyTos,followers,friends));


    }
}
