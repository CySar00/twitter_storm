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
public class RemoveAnyDuplicateValuesOfAuthors extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<Long>>IDs;
    Map<String,List<String>>tweets;
    Map<String,List<Date>>dates;
    Map<String,List<Long>>inReplyTos;
    Map<String,List<String>>followers;
    Map<String,List<String>>friends;
    Set<String> unique;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","IDS","TWEETS","DATES","IN_REPLY_TOS","FOLLOWERS","FRIENDS"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        IDs=new HashMap<String, List<Long>>();
        tweets=new HashMap<String, List<String>>();
        dates=new HashMap<String, List<Date>>();

        inReplyTos=new HashMap<String, List<Long>>();
        followers=new HashMap<String, List<String>>();
        friends=new HashMap<String, List<String>>();

        unique=new HashSet<String>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);

        List<Long>ids=(List<Long>)input.getValue(1);
        List<String>tweets1=(List<String>)input.getValue(2);
        List<Date>dates1=(List<Date>)input.getValue(3);
        List<Long>inReplyTos1=(List<Long>)input.getValue(4);
        List<String>followers1=(List<String>)input.getValue(5);
        List<String>friends1=(List<String>)input.getValue(6);

        List<Long>IDs=this.IDs.get(author);
        if(IDs==null){
            IDs=new ArrayList<Long>();
        }
        IDs=ids;
        this.IDs.put(author,IDs);

        List<String>tweets=this.tweets.get(author);
        if(tweets==null){
            tweets=new ArrayList<String>();
        }
        tweets=tweets1;
        this.tweets.put(author,tweets);

        List<Date>dates=this.dates.get(author);
        if(dates==null){
            dates=new ArrayList<Date>();
        }
        dates=dates1;
        this.dates.put(author,dates);

        List<Long>inReplyTos=this.inReplyTos.get(author);
        if(inReplyTos==null){
            inReplyTos=new ArrayList<Long>();
        }
        inReplyTos=inReplyTos1;
        this.inReplyTos.put(author,inReplyTos);

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

        if (!unique.contains(author)) {
            collector.emit(input,new Values(author,IDs,tweets,dates,inReplyTos,followers,friends));
            unique.add(author);
        }



    }
}
