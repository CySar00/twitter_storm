package Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank.GaussianRank_For_Centroid_Index_Equals_1;

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
public class ReadValuesFromEMFile extends BaseRichBolt {
    private  OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","LIST_OF_MEANS","LIST_OF_SIGMAS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);

        String[]substrings=line.split("\\[");
        String author=substrings[1].replace(",","");

        String meansString=substrings[2].replace("],","");

        String[]splitMeans=meansString.split(",");
        List<Double> means=new ArrayList<Double>();
        for(int i=0;i<splitMeans.length;i++){
            means.add(Double.valueOf(splitMeans[i]));
        }

        String sigmaString=substrings[3].replace("]]","");
        String[]splitSigma=sigmaString.split(",");
        List<Double>sigma=new ArrayList<Double>();
        for(int i=0;i<splitSigma.length;i++){
            sigma.add(Double.valueOf(splitSigma[i]));
        }

        collector.emit(input,new Values(author,means,sigma));


    }
}
