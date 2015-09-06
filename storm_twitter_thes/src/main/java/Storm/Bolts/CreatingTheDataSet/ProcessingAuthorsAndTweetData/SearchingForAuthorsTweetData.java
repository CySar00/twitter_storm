package Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mongodb.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by christina on 7/23/15.
 */
public class SearchingForAuthorsTweetData extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,String>tweet;
    Map<String,Long>ID;
    Map<String,Date>date;
    Map<String,List<String>>followers;
    Map<String,List<String>>friends;
    Map<String,Long>inReplyTo;



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","ID","TWEET","DATE","IN_REPLY_TO","FOLLOWERS","FRIENDS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        tweet=new HashMap<String, String>();
        ID=new HashMap<String, Long>();
        date=new HashMap<String, Date>();
        followers=new HashMap<String, List<String>>();
        friends=new HashMap<String, List<String>>();
        inReplyTo=new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);

        String tweet1=input.getString(1);
        Long ID1=input.getLong(2);
        Date date1=(Date)input.getValue(3);
        Long inReplyTo1=input.getLong(4);
        List<String>follwers1=(List<String>)input.getValue(5);
        List<String>friends1=(List<String>)input.getValue(6);

        String tweet=this.tweet.get(author);
        if(tweet==null){
            tweet="";
        }
        tweet=tweet1;
        this.tweet.put(author,tweet);

        Long ID=this.ID.get(author);
        if(ID==null){
            ID=0L;
        }
        ID=ID1;
        this.ID.put(author,ID);

        Date date=this.date.get(author);
        if(date==null){
            date=new Date();
        }
        date=date1;
        this.date.put(author,date);

        Long inReplyTo=this.inReplyTo.get(author);
        if(inReplyTo==null){
            inReplyTo=0L;
        }
        inReplyTo=inReplyTo1;
        this.inReplyTo.put(author,inReplyTo);

        List<String>followers=this.followers.get(author);
        if(followers==null){
            followers=new ArrayList<String>();
        }
        followers=follwers1;
        this.followers.put(author,followers);

        List<String>friends=this.friends.get(author);
        if(friends==null){
            friends=new ArrayList<String>();
        }
        friends=friends1;
        this.friends.put(author,friends);

        String authorsFromCassandraDB=Storm.Databases.CassandraDB.Preprocessing.CassandraSchemaForAuthors.readAuthorsFromCassandraDB();
        if(authorsFromCassandraDB.contains(author)){
           // System.out.println(author+" "+tweet+" "+ID+" "+date+" "+inReplyTo+" "+followers+" "+friends);
            collector.emit(input,new Values(author,ID,tweet,date,inReplyTo,followers,friends));
        }
      //  collector.ack(input);


    }
}
