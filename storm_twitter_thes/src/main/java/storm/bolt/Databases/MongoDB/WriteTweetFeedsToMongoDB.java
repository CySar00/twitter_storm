package storm.bolt.Databases.MongoDB;

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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 3/23/15.
 */
public class WriteTweetFeedsToMongoDB extends BaseRichBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {


    }

    @Override
    public void execute(Tuple input){
        try{
            MongoClient client=new MongoClient(new ServerAddress("localhost"));
            DB db=client.getDB("storm");
            final DBCollection collection=db.getCollection("tweet_info");



            Status status=(Status)input.getValue(0);
            ResponseList<User>followers=(ResponseList<User>)input.getValue(1);
            ResponseList<User>friends=(ResponseList<User>)input.getValue(2);

            BasicDBObject dbObject=new BasicDBObject();
            dbObject.put("tweetID",status.getId());
            dbObject.put("tweetAuthor",status.getUser().getScreenName());
            dbObject.put("tweet",status.getText());

            dbObject.put("tweetCreatedAt",status.getCreatedAt());
            dbObject.put("tweetHashTags",serializeHashTags(status.getHashtagEntities()));
            dbObject.put("tweetURLs",serializeURLs(status.getURLEntities()));
            dbObject.put("tweetUserMentions",serializeUserMentions(status.getUserMentionEntities()));

            dbObject.put("inReplyToUserId",status.getInReplyToUserId());

            dbObject.put("followers",serializeUsers(followers));
            dbObject.put("friends",serializeUsers(friends));
            collection.insert(dbObject);

            Runtime runtime=Runtime.getRuntime();
            DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd");
            Date date=new Date();
            String xyz=dateFormat.format(date);

            String query="mongoexport --db storm --collection tweet_info --fields tweetID,tweetAuthor,tweet,tweetCreatedAt,tweetHashTags,tweetURLs,tweetUserMentions,inReplyToUserId,followers,friends -o mongoData.txt";
            try{
                Process process=runtime.exec(query);
                System.out.println(query);
                System.out.println(xyz);



            }catch (IOException ex){
                ex.printStackTrace();

            }



            client.close();


        }catch (UnknownHostException ex){
            ex.printStackTrace();
        }


    }

    private List<String>serializeURLs(URLEntity[]urlEntities){
        List<String>list=new CopyOnWriteArrayList<String>();
        for(int i=0;i<urlEntities.length;i++){
            list.add(urlEntities[i].getText());
        }
        return list;
    }

    private List<String>serializeHashTags(HashtagEntity[]hashtagEntities){
        List<String>list=new CopyOnWriteArrayList<String>();
        for(int i=0;i<hashtagEntities.length;i++){
            list.add(hashtagEntities[i].getText());
        }
        return list;
    }

    private List<String>serializeUserMentions(UserMentionEntity[]userMentionEntities){
        List<String>list=new CopyOnWriteArrayList<String>();
        for(int i=0;i<userMentionEntities.length;i++){
            list.add(userMentionEntities[i].getScreenName());
        }
        return list;
    }

    private List<String>serializeUsers(ResponseList<User>users){
        List<String>list=new CopyOnWriteArrayList<String>();
        for(User user:users){
            list.add(user.getScreenName());
        }
        return list;
    }








}
