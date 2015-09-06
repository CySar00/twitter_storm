package Storm.Bolts.ClusteringTechniques.Gaussian;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/23/15.
 */
public class ProcessIndexedLinesOfRandomNumbers extends BaseRichBolt {
    private OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","LIST"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        int index=input.getInteger(0);
        String line=input.getString(1);

        //System.out.println(line);
        int index1=line.indexOf("[[");
        int index2=line.indexOf("]]");

        String subString=line.substring(index1+2,index2);
        System.out.println(subString);
        String[]split=subString.split(",");
        List<Double>list=new ArrayList<Double>();
        for(int i=0;i<split.length;i++){
            list.add(Double.valueOf(split[i]));
        }
        collector.emit(input,new Values(String.valueOf(index),list));

    }
}
