package storm.bolt.GaussianRankAndMixtureModel.DataMaps;

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
 * Created by christina on 4/2/15.
 */
public class CreateDataSetFromAKCluster extends BaseRichBolt {
    OutputCollector collector;

    Map<String ,List<Double>>fuckingMap=new HashMap<String,List<Double>>();



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("FUCKING_AUTHOR","FUCKING_VECTOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lineFromFile=input.getString(0);

        int firstIndex=lineFromFile.indexOf("[1");
        int lastIndex=lineFromFile.indexOf("]]");

        String dataLine=lineFromFile.substring(firstIndex+3,lastIndex+1);
        String dataLine1=dataLine.replace(", [","[");
       // String []fuckingDataInfo=dataLine1.split("\\[");

        String theFuckingAuthor= StringUtils.substringBefore(dataLine1,"[");
        String theFuckingAuthorsFuckingFeatures=StringUtils.substringBetween(dataLine1, "[", "]");

        final List<Double>fuckingListOfFeatures=new ArrayList<Double>();
        //System.out.println(theFuckingAuthor+" "+theFuckingAuthorsFuckingFeatures);
        String[]aFuckingFeature=theFuckingAuthorsFuckingFeatures.split(",");
        for(int i=0;i<aFuckingFeature.length;i++){
            fuckingListOfFeatures.add(Double.parseDouble(aFuckingFeature[i]));
        }

        if(!fuckingMap.containsKey(theFuckingAuthor)){
            collector.emit(input,new Values(theFuckingAuthor,fuckingListOfFeatures));
            fuckingMap.put(theFuckingAuthor,fuckingListOfFeatures);
        }
        collector.ack(input);

    }
}
