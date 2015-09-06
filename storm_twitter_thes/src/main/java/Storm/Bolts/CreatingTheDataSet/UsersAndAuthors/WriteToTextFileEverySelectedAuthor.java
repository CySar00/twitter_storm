package Storm.Bolts.CreatingTheDataSet.UsersAndAuthors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 7/10/15.
 */
public class WriteToTextFileEverySelectedAuthor extends BaseRichBolt {
    private OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);
      //  System.out.println(line);

        int firstIndex=line.indexOf("[[");
        int lastIndex=line.indexOf("]]");

        String subString=line.substring(firstIndex+2);
        System.out.println(subString);
        String subString1=subString.replace("]]","");
        System.out.println(subString1);
        String[]splitString=subString1.split(",");
        List<String>authors=new ArrayList<String>();

        for(int i=0;i<splitString.length;i++){
            authors.add(splitString[i]);
        }

        for(String author:authors){
            System.out.println(author);
            collector.emit(input,new Values(author));
        }

       // collector.ack(input);
    }
}


