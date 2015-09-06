package Storm.Bolts.ClusteringTechniques.Gaussian;

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
 * Created by christina on 7/21/15.
 */
public class ReadRandomNumbersFromFile extends BaseRichBolt {
    OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("LIST"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);
     //   System.out.println(line);

        int index1=line.indexOf("[[");
        int index2=line.indexOf("]]");

        String subline=line.substring(index1+2);
      //  System.out.println(subline);
        String subline1=subline.replace("]]","");
      //  System.out.println(subline1);


        List<Double> list=new ArrayList<Double>();

        String[]split=subline1.split(",");
        for(int i=0;i<split.length;i++){
            list.add(Double.valueOf(split[i]));
        }
        System.out.println(list);


        collector.emit(input,new Values(list));
    }
}
