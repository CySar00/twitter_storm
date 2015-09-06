package Storm.Bolts.ClusteringTechniques.FusionLists.Example;

import Storm.Bolts.ClusteringTechniques.FusionLists.Functions.RankAggregation;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/31/15.
 */
public class Example1 extends BaseRichBolt {
    private OutputCollector collector;

    RankAggregation<String>rankAggregation;
    Map<String,Map<String,Double>>scores;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        rankAggregation=new RankAggregation<String>();
        scores=new HashMap<String, Map<String, Double>>();
    }

    @Override
    public void execute(Tuple input) {
        Map<String,double[]>map=(Map<String,double[]>)input.getValue(0);

        for(String key:map.keySet()){
            scores.put(key,new HashMap<String, Double>());
            String temp=null;
            for(int i=0;i<map.get(key).length;i++){
                temp="Metric ".concat(String.valueOf(i));
                scores.get(key).put(temp,map.get(key)[i]);
            }
        }

        Map<String,Double>[]aux=rankAggregation.processMap(scores);
        Map<String,Double>borda=rankAggregation.bordaFusion(aux);

        for(String key:borda.keySet()){
            System.out.println(key+" --->>>>"+borda.get(key));
        }
    }
}
