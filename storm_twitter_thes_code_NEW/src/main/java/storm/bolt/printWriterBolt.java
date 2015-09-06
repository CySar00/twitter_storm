package storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by christina on 10/7/14.
 */
public class printWriterBolt extends BaseBasicBolt {

    private String prefix;

    public printWriterBolt(String prefix){
        this.prefix=prefix;

    }

    @Override
    public void execute(Tuple tuple,BasicOutputCollector collector){
        System.err.println(prefix + ":" + tuple);
        //this bolt doesn't emit any values further.it only prints values to the console

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        //this bolt doesn't emit any values,hence it doesnt declare any fields for it's output
    }


}
