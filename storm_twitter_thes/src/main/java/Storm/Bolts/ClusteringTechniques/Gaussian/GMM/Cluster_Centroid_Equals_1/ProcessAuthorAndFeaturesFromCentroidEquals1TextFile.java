package Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Cluster_Centroid_Equals_1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/20/15.
 */
public class ProcessAuthorAndFeaturesFromCentroidEquals1TextFile extends BaseRichBolt {

    private OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","LIST"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);
        int initialIndex=line.indexOf("[1,");
        int lastIndex=line.indexOf("]]");

        String subStringOfLine=line.substring(initialIndex+3,lastIndex+1);
        System.out.println(subStringOfLine);
        String[]splitted=subStringOfLine.split("\\[");

        String author=splitted[0].replace(",","");
        System.out.println(author);

        String values1=splitted[1].replace("[","");
        String values2=values1.replace("]","");
        System.out.println(values2);

        List<Double> list=new ArrayList<Double>();
        String[]splittedValues2=values2.split(",");
        for(int i=0;i<splittedValues2.length;i++){
            list.add(Double.valueOf(splittedValues2[i]));
        }

        System.out.println(author+" "+list);
        collector.emit(input,new Values(author,list));

    }

}
