package storm.bolt.DataProcessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mysql.jdbc.PacketTooBigException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 3/25/15.
 */
public class SplitLinesIntoTupleValueFields extends BaseRichBolt {
    OutputCollector collector;

    private String author;private String tweet;
    private Date createdAt;

    private Long tweetID;private Long inReplyToUserID;
    private List<String> followers; private List<String>friends;private List<String>hashtags;
    private List<String>URLs;private List<String>userMentions;

    private SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","ID","TWEET","CREATED_AT","#TAGS","URLS","@USER","IN_REPLY","FOLLOWERS","FRIENDS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] tupleFields=line.split(",");

        if(line!=null) {


            for (int i = 0; i < tupleFields.length; i++) {

                if (tupleFields[i].startsWith("\"followers\"")) {
                    followers = removeTwoStringsFromWordAndCastToList(tupleFields[i], "\"followers\":[", "]", ",");
                }

                if (tupleFields[i].startsWith("\"friends\"")) {
                    friends = removeTwoStringsFromWordAndCastToList(tupleFields[i], "\"friends\":[", "]", ",");
                }

                if (tupleFields[i].startsWith("\"tweetHashTags\":[")) {
                    hashtags = removeTwoStringsFromWordAndCastToList(tupleFields[i], "\"tweetHashTags\":[", "]", ",");
                }

                if (tupleFields[i].startsWith("\"tweetURLs\":[")) {
                    URLs = removeTwoStringsFromWordAndCastToList(tupleFields[i], "\"tweetURLs\":[", "]", ",");

                }

                if (tupleFields[i].startsWith("\"tweetUserMentions\":[")) {
                    userMentions = removeTwoStringsFromWordAndCastToList(tupleFields[i], "\"tweetUserMentions\":[", "]}", ",");
                }

                if (tupleFields[i].startsWith("\"tweetID\":{\"$numberLong\":\"")) {
                    tweetID = removeTwoStringsFromWordAndCastToLong(tupleFields[i], "\"tweetID\":{\"$numberLong\":\"", "\"}");
                }

                if (tupleFields[i].startsWith("\"inReplyToUserId\":{\"$numberLong\":\"")) {
                    inReplyToUserID = removeTwoStringsFromWordAndCastToLong(tupleFields[i], "\"inReplyToUserId\":{\"$numberLong\":\"", "\"}");
                }

                if (tupleFields[i].startsWith("\"tweetCreatedAt\":{\"$date\":")) {
                    String temp = removeTwoStringsFromWord(tupleFields[i], "\"tweetCreatedAt\":{\"$date\":\"", "\"}");
                    try {
                        createdAt = dateFormat.parse(temp);
                    } catch (ParseException pEx) {
                        pEx.printStackTrace();
                    }
                }

                if (tupleFields[i].startsWith("\"tweetAuthor\":")) {
                    author = removeTwoStringsFromWord(tupleFields[i], "\"tweetAuthor\":\"", "\"");
                }

                if (tupleFields[i].startsWith("\"tweet\":\"")) {
                    tweet = removeTwoStringsFromWord(tupleFields[i], "\"tweet\":\"", "\"");
                }

                if(tweetID!=null && author!=null && tweet!=null &&  createdAt!=null && hashtags!=null && URLs!=null && userMentions!=null && inReplyToUserID!=null && followers!=null && friends!=null) {
                    collector.emit(input,new Values(author,tweetID, tweet, createdAt, hashtags, URLs, userMentions, inReplyToUserID, followers, friends));
                }else{
                    collector.fail(input);
                }

            }
        }
        collector.ack(input);

    }


    private String removeOneWordFromString(String string,String remove){
        return string.replace(remove,"");
    }

    private String removeTwoStringsFromWord(String string,String remove1,String remove2){
        String temp=string.replace(remove1,"");
        return temp.replace(remove2,"");
    }

    private List<String>removeTwoStringsFromWordAndCastToList(String word,String remove1,String remove2,String split){
        String temp1=word.replace(remove1,"");
        String temp2=temp1.replace(remove2,"");

        String []temp=temp2.split(split);

        return Arrays.asList(temp);
    }

    private Long removeTwoStringsFromWordAndCastToLong(String word,String remove1,String remove2){
        String temp1=word.replace(remove1,"");
        String temp2=temp1.replace(remove2,"");
        return Long.valueOf(temp2);

    }



}
