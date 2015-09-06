package Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/10/15.
 */
public class FromMongoDBToFurtherProcessing extends BaseRichBolt {
    OutputCollector collector;

    Long ID;
    Long inReplyToAuthorID;

    List<String> followers;
    List<String> friends;
    List<String> URLs;
    List<String> hashtags;
    List<String> atUsers;

    Date date;
    String author;
    String tweet;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME", "ID", "TWEET", "DATE", "IN_REPLY_TO_AUTHOR_ID","FOLLOWERS","FRIENDS"));


    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {


        String lineFromFile = input.getString(0);


        String[] splitLineFromFileIntoFields = lineFromFile.split(",");
        for (int i = 0; i < splitLineFromFileIntoFields.length; i++) {
            if (splitLineFromFileIntoFields[i].startsWith("{\"ID\":{\"$numberLong\":")) {
                ID = castModifiedStringToLong(splitLineFromFileIntoFields[i], "{\"ID\":{\"$numberLong\":\"", "\"}");
            }
            if (splitLineFromFileIntoFields[i].startsWith("\"inReplyToAuthorID\":{\"$numberLong\":\"")) {
                inReplyToAuthorID = castModifiedStringToLong(splitLineFromFileIntoFields[i], "\"inReplyToAuthorID\":{\"$numberLong\":\"", "\"}");
                }

            if (splitLineFromFileIntoFields[i].startsWith("\"author\"")) {
                author = removeTwoWordsFromString(splitLineFromFileIntoFields[i], "\"author\":\"", "\"");
            }


            if (splitLineFromFileIntoFields[i].startsWith("\"tweet\":\"")) {
                tweet = removeTwoWordsFromString(splitLineFromFileIntoFields[i], "\"tweet\":\"", "\"}");
            }

            if (splitLineFromFileIntoFields[i].startsWith("\"followers\"")) {
                followers = castModifiedStringIntoList(splitLineFromFileIntoFields[i], "\"followers\":[", "]", ",");
            }

            if (splitLineFromFileIntoFields[i].startsWith("\"friends\"")) {
                friends = castModifiedStringIntoList(splitLineFromFileIntoFields[i], "\"friends\":[", "]", ",");
            }


            if (splitLineFromFileIntoFields[i].startsWith("\"date\":{\"$date\":\"")) {
                String tempString = removeTwoWordsFromString(splitLineFromFileIntoFields[i], "\"date\":{\"$date\":\"", "\"}");
                try {
                    date = dateFormat.parse(tempString);
                } catch (ParseException ex) {
                    ex.printStackTrace();
                }
            }
        }
        collector.emit(input,new Values(author,ID,tweet,date,inReplyToAuthorID,followers,friends));

       // collector.ack(input);

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
        tempString2=initialString.replace(str2,"");

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
