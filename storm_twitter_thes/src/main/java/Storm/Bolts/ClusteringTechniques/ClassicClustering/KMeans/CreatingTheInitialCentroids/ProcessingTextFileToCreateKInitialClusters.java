package Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.CreatingTheInitialCentroids;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class ProcessingTextFileToCreateKInitialClusters extends BaseRichBolt {
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

        List<Double> metricsAndFeatures=new ArrayList<Double>();

        int initialIndex=line.indexOf("[");
        int lastIndex=line.indexOf("]");

        String subLine= StringUtils.substring(line,initialIndex+1,lastIndex);
        String[] subStringsOfSubLine=subLine.split(",");

        for(int i=1;i<subStringsOfSubLine.length;i++){
            metricsAndFeatures.add(Double.valueOf(subStringsOfSubLine[i]));
        }
        System.out.println(index+" "+metricsAndFeatures+" "+metricsAndFeatures.size());
        collector.emit(input,new Values(index,metricsAndFeatures));


    }
}
