package Storm.Bolts.ClusteringTechniques.FusionLists.Example;

import Storm.Bolts.ClusteringTechniques.FusionLists.Functions.RankAggregation;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by christina on 7/31/15.
 */
public class Example extends BaseRichBolt {
    private OutputCollector collector;

    RankAggregation<String>rankAggregation;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.rankAggregation=new RankAggregation<String>();
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        Map<String,Map<String,Double>>scores=(Map<String,Map<String,Double>>)input.getValue(0);

        Map<String,Double>[]aux=rankAggregation.processMap(scores);
        Map<String,Double>bordaResult=rankAggregation.bordaFusion(scores);
        for(String key:bordaResult.keySet()){
            System.out.println(key+" -->>"+bordaResult.get(key));
        }
    }
}
