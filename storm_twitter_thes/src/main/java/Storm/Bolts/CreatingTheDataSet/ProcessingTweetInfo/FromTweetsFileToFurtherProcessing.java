package Storm.Bolts.CreatingTheDataSet.ProcessingTweetInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 7/16/15.
 */
public class FromTweetsFileToFurtherProcessing extends BaseRichBolt {
    OutputCollector collector;

    String author;
    Long ID;
    String tweet;
    Long inReplyTo;
    List<String>followers;
    List<String>friends;
    Date date;


    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","TWEET" ,"ID" ,"DATE","IN_REPLY_TO","FOLLOWERS","FRIENDS"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lineFromFile = input.getString(0);

        Long ID;
        Long inReplyToAuthorID;

        List<String> followers;
        List<String> friends;
        List<String> URLs;
        List<String> hashtags;
        List<String> atUsers;

        Date date;
        String author="";
        String tweet;

            String[] splitLineFromFileIntoFields = lineFromFile.split(",");

            for (int i = 0; i < splitLineFromFileIntoFields.length; i++) {

                if (splitLineFromFileIntoFields[i].startsWith("\"tweetID\":{\"$numberLong\":")) {
                    ID = castModifiedStringToLong(splitLineFromFileIntoFields[i],"\"tweetID\":{\"$numberLong\":\"","\"}}");
                    this.ID=ID;
                }

                if (splitLineFromFileIntoFields[i].startsWith("\"inReplyToUserId\":{\"$numberLong\":\"")) {
                    inReplyToAuthorID = castModifiedStringToLong(splitLineFromFileIntoFields[i],"\"inReplyToUserId\":{\"$numberLong\":\"","\"}");
                    this.inReplyTo=inReplyToAuthorID;
                }

                if (splitLineFromFileIntoFields[i].startsWith("\"followers\":[")) {
                    followers = castModifiedStringIntoList(splitLineFromFileIntoFields[i],"\"followers\":[", "]", ",");
                    this.followers=followers;
                }

                if (splitLineFromFileIntoFields[i].startsWith("\"friends\"")) {
                    friends = castModifiedStringIntoList(splitLineFromFileIntoFields[i], "\"friends\":[", "]", ",");
                    this.friends=friends;
                }

                if (splitLineFromFileIntoFields[i].startsWith("\"tweetAuthor\"")) {
                    this.author = removeTwoWordsFromString(splitLineFromFileIntoFields[i], "\"tweetAuthor\":\"", "\"");
                }

                if (splitLineFromFileIntoFields[i].startsWith("\"tweet\":\"")) {
                    tweet = removeTwoWordsFromString(splitLineFromFileIntoFields[i], "\"tweet\":\"", "\"}");
                    this.tweet=tweet;
                }


                if (splitLineFromFileIntoFields[i].startsWith("\"tweetCreatedAt\":{\"$date\":\"")) {
                    String tempString = removeTwoWordsFromString(splitLineFromFileIntoFields[i], "\"tweetCreatedAt\":{\"$date\":\"", "\"}");
                    try {
                        date = dateFormat.parse(tempString);
                        this.date=date;
                    } catch (ParseException ex) {
                        ex.printStackTrace();
                    }

                }

        }
      //  System.out.println(author+" "+ID+" "+tweet+" "+date+" "+inReplyToAuthorID+" "+" "+followers+" "+friends);
        collector.emit(input,new Values(this.author,this.tweet,this.ID,this.date,this.inReplyTo,this.followers,this.friends));
    //    collector.ack(input);
    }



    private String removeOneWordFromString(String string,String remove){
        return string.replace(remove,"");
    }


    private  String removeTwoWordsFromString(String initialString,String str1,String str2){
        String tempString;String finalString;

        tempString=initialString.replace(str1,"");
        finalString=tempString.replace(str2,"");
        return  finalString;
    }

    private List<String> castModifiedStringIntoList(String initialString,String str1,String str2,String split){
        String tempString1;String tempString2;
        tempString1=initialString.replace(str1,"");
        tempString2=tempString1.replace(str2,"");

        String []arrayString=tempString2.split(split);
        return Arrays.asList(arrayString);

    }





    private Long castStringToLong(String str){
        return Long.valueOf(str);
    }

    private Long castModifiedStringToLong(String initialString,String str1,String str2){
        String tempString1;String tempString2;

        tempString1=initialString.replace(str1,"");
        tempString2=tempString1.replace(str2,"");

        return Long.valueOf(tempString2);
    }
}



