package Storm.Bolts.ClusteringTechniques.FusionLists;

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
 * Created by christina on 7/29/15.
 */
public class ProcessDataFromFuzzyCluster0 extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("CLUSTER_INDEX","INDEX","VALUES"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);
      //  System.out.println(line);

        int index1=line.indexOf("[0,");
        int index2=line.indexOf("]]");

        String substring=line.substring(index1+3,index2);
        String[]split1=substring.split("\\[");

        String key=split1[0].replace(",","");
        String[] values=split1[1].split(",");
        List<Double> list=new ArrayList<Double>();
        for(int i=0;i<values.length;i++){
            list.add(Double.valueOf(values[i]));
        }
        collector.emit(input,new Values(0,key,list));
     //   collector.ack(input);

    }
}
