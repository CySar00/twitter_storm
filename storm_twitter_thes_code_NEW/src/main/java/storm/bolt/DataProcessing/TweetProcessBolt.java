package storm.bolt.DataProcessing;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.*;

import java.util.Date;

/**
 * Created by christina on 5/30/15.
 */
public class TweetProcessBolt extends BaseBasicBolt{

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID","USERNAME","TEXT","#TAGS","URLS","@USER","IN_REPLY_TO_USER","CREATED_AT","FOLLOWERS","FRIENDS"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Status status=(Status)input.getValue(0);
        ResponseList<User>followers=(ResponseList<User>)input.getValue(1);
        ResponseList<User>friends=(ResponseList<User>)input.getValue(2);

        if(status!=null && followers!=null && friends!=null){
            Long ID=status.getId();
            String username=status.getUser().getScreenName();
            String text=status.getText();

            HashtagEntity[] hashtags=status.getHashtagEntities();
            URLEntity []urls=status.getURLEntities();
            UserMentionEntity[]userMentions=status.getUserMentionEntities();

            Long inReplyToUserID=status.getInReplyToUserId();
            Date createdAt=status.getCreatedAt();

            collector.emit(new Values(ID,username,text,hashtags,urls,userMentions,inReplyToUserID,createdAt,followers,friends));
        }
    }
}
