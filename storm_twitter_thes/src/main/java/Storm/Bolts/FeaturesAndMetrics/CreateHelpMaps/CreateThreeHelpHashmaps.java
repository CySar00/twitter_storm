package Storm.Bolts.FeaturesAndMetrics.CreateHelpMaps;

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
public class CreateThreeHelpHashmaps extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<String>>tweets;
    Map<String,List<Long>>IDs;
    Map<String,List<Date>>dates;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","MAP_OF_IDS","MAP_OF_TWEETS","MAP_OF_DATES"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        IDs=new HashMap<String, List<Long>>();
        tweets=new HashMap<String, List<String>>();
        dates=new HashMap<String, List<Date>>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        List<Long> ids=(List<Long>)input.getValue(1);

        List<String>tweets1=(List<String>)input.getValue(2);
        List<Date>dates1=(List<Date>)input.getValue(3);

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

        collector.emit(input,new Values(author,this.IDs,this.tweets,this.dates));
    }
}
