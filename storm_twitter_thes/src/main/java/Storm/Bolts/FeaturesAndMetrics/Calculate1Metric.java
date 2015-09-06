package Storm.Bolts.FeaturesAndMetrics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.trident.testing.Split;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/16/15.
 */
public class Calculate1Metric extends BaseRichBolt {
    private OutputCollector collector;
    Map<String,Integer>OT1;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","OT1"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        OT1=new HashMap<String, Integer>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        String tweet=input.getString(2);

        Integer OT1=this.OT1.get(author);
        if(OT1==null){
            OT1=0;
        }

        if(!tweet.startsWith("@") && !tweet.startsWith("RT")){
            OT1+=1;
        }
        System.out.println(author+" "+OT1);
        this.OT1.put(author,OT1);

        collector.emit(input,new Values(author,OT1));
        //collector.ack(input);
     }



}
