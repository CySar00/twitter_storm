package storm.bolt.FusionLists.ReadData;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 4/28/15.
 */
public class ProcessDataFromTextFile extends BaseRichBolt {
    OutputCollector collector;
    Map<String,List<Double>>map=new HashMap<String, List<Double>>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","FEATURES"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lineFromTextFile=input.getString(0);
       // System.out.println(lineFromTextFile);

        int firstIndexOfLine=lineFromTextFile.indexOf("[1");
        int lastIndexOfLine=lineFromTextFile.indexOf("]]");

        String dataFromLine=lineFromTextFile.substring(firstIndexOfLine+3,lastIndexOfLine+1);
        String dataFromLine1=dataFromLine.replace(",[","[");

        String theAuthor= StringUtils.substringBefore(dataFromLine1,"[");
        String theFeatures=StringUtils.substringBetween(dataFromLine1,"[","]");

        final List<Double>theFeaturesInAList=new ArrayList<Double>();
        String []theFeaturesInAVector=theFeatures.split(",");
        for(int i=0;i<theFeaturesInAVector.length;i++){
            theFeaturesInAList.add(Double.parseDouble(theFeaturesInAVector[i]));
        }

        System.out.println(theAuthor+" "+theFeaturesInAList);

        if(!map.containsKey(theAuthor)){
            collector.emit(input,new Values(theAuthor,theFeaturesInAList));
            map.put(theAuthor,theFeaturesInAList);
        }
        collector.ack(input);


        
    }
}
