package Storm.Bolts.Databases.MongoDB;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import twitter4j.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 7/10/15.
 */
public class WriteTheAuthorsTweetFeedsToMongoDB extends BaseRichBolt {
    Set<Long> IDs=new HashSet<Long>();


    Map<Long,String>authors=new HashMap<Long, String>();
    Map<Long,String>tweets=new HashMap<Long, String>();
    Map<Long,List<String>>hashtags=new HashMap<Long,List< String>>();
    Map<Long,List<String>>URLs=new HashMap<Long, List<String>>();
    Map<Long,Long>inReply=new HashMap<Long, Long>();
    Map<Long,List<String>>followers=new HashMap<Long, List<String>>();
    Map<Long,List<String>>friends=new HashMap<Long, List<String>>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {


    }

    @Override
    public void execute(Tuple input) {
        try {
            MongoClient client = new MongoClient(new ServerAddress("localhost"));
            DB db = client.getDB("storm_NEW_");
            final DBCollection collection = db.getCollection("Author_And_Tweet_Data0");

            String author=input.getString(0);
            Long ID=input.getLong(1);
            String text=input.getString(2);
            Date Tdate=(Date)input.getValue(3);
            Long inReplyToAuthorID=input.getLong(4);
            List<String>followers=(List<String>)input.getValue(5);
            List<String>friends=(List<String>)input.getValue(6);

            if(!IDs.contains(ID)){

            BasicDBObject dbObject = new BasicDBObject();
            dbObject.put("author", author);
            dbObject.put("ID", ID);
            dbObject.put("tweet", text);
            dbObject.put("date", Tdate);
            dbObject.put("inReplyToAuthorID", inReplyToAuthorID);
            dbObject.put("followers",followers);
            dbObject.put("friends",friends);

            collection.insert(dbObject);

            Runtime runtime = Runtime.getRuntime();
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
            Date date = new Date();
            String xyz = dateFormat.format(date);

            String query = "mongoexport --db storm_NEW_ --collection Author_And_Tweet_Data0 --fields author,ID,tweet,date,inReplyToAuthorID,followers,friends -o author_and_tweet_data.txt";
            try {
                Process process = runtime.exec(query);
                System.out.println(query);
                System.out.println(xyz);

            } catch (IOException ex) {
                        ex.printStackTrace();

            }
                IDs.add(ID);
            }

            client.close();


        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }


    }



}

