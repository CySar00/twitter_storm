package Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.RandomNumbers.InitialCentroids;

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
 * Created by christina on 7/27/15.
 */
public class ProcessThe3LinesToCreateTheInitialClusters extends BaseRichBolt {
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
        Integer index=input.getInteger(0);
        String line=input.getString(1);

        int index1=line.indexOf("[[");
        int index2=line.indexOf("]]");

        String substring=line.substring(index1+2,index2);
        String []split=substring.split(",");
        List<Double> list=new ArrayList<Double>();
        for(int i=0;i<split.length;i++){
            list.add(Double.valueOf(split[i]));
        }
        collector.emit(input,new Values(index,list));


    }
}
