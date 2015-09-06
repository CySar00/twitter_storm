package Storm.Bolts.ClusteringTechniques.ClassicClustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import me.prettyprint.cassandra.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class ProcessingDataSetForClassicClustering extends BaseRichBolt {
    private OutputCollector collector;
    List<Double> list;
    double[]vector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","LIST","VECTOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        list=new ArrayList<Double>();
    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);

        List<Double>list=new ArrayList<Double>();

        int firstIndex=line.indexOf("[");
        int lastIndex=line.indexOf("]");

        String subLine=line.substring(firstIndex+1,lastIndex);
      //  System.out.println(subLine);
        String[]subStrings=subLine.split(",");

        String author=subStrings[0];
        for(int i=1;i<subStrings.length;i++){
            list.add(Double.valueOf(subStrings[i]));
        }

        vector= Doubles.toArray(list);
    //    System.out.println(author+" "+list+" "+vector);
        collector.emit(input, new Values(author, list, vector));
        collector.ack(input);









    }
}
