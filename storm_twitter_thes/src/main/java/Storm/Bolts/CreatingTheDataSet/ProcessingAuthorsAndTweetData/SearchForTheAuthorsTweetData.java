package Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;

/**
 * Created by christina on 7/16/15.
 */
public class SearchForTheAuthorsTweetData extends BaseRichBolt{
    OutputCollector collector;
    List<String>authors;
    List<String>temp;

    String author1;String author2;

    Map<String,Long>IDs;
    Map<String,String>tweet;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME"));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector=collector;
        authors=new ArrayList<String>();
        temp=new ArrayList<String>();

        IDs=new HashMap<String, Long>();
        tweet=new HashMap<String, String>();
    }

    @Override
    public void execute(Tuple input) {
        final String sourceComponent = input.getSourceComponent();




        if ("PROCESS_AUTHORS".equals(sourceComponent)) {
            // System.out.println("fuck me");
            author1 = input.getString(0);
            authors.add(author1);
        }




        if ("PROCESS_TWEETS_FROM_MONGODB".equals(sourceComponent)) {
            author2 = input.getString(0);
            // System.out.println("right?");
            temp.add(author2);

            String tweet1=input.getString(1);
            String tweet=this.tweet.get(author2);
            if(tweet==null){
                tweet="";
            }
            tweet=tweet1;
            this.tweet.put(author2,tweet);

            if(authors.contains(author2)){
                System.out.println(author2+" "+tweet);
            }

        }






    }






}
