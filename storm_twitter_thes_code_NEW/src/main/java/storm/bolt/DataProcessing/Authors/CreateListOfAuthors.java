package storm.bolt.DataProcessing.Authors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 3/26/15.
 */
public class CreateListOfAuthors extends BaseRichBolt{
    private OutputCollector collector;

    List<String>authors=new CopyOnWriteArrayList<String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","ID","TEXT","DATE","#TAGS","URLS","@USER","IN_REPLY_TO","FOLLOWERS","FRIENDS"));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);

        int startIndex=line.indexOf("[[");
        int lastIndex=line.indexOf("]]");

        String listOfAuthors=line.substring(startIndex+1,lastIndex);
    //    System.out.println(listOfAuthors);

        String temp=listOfAuthors.replace("[","");
        String[]theAuthors=temp.split(", ");
        authors=Arrays.asList(theAuthors);

        //System.out.println(authors);
        HashSet<String>everyAuthor=new HashSet<String>();
        if(!authors.isEmpty()) {
            for(String anAuthor:theAuthors){
                if(!everyAuthor.contains(anAuthor)) {
                    //System.out.println(anAuthor);
                    collector.emit(input, new Values(anAuthor, 0L, null, null, null, null, null, 0L, null, null));
                    everyAuthor.add(anAuthor);
                }
            }

          // collector.emit(input, new Values(authors));
        }
        collector.ack(input);

    }


}

