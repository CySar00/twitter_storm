package Storm.Bolts.CreatingTheDataSet.UsersAndAuthors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by christina on 7/16/15.
 */
public class ProcessAuthors extends BaseRichBolt {
    OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lineFromFile=input.getString(0);

        int initialIndex=lineFromFile.indexOf("[");
        int lastIndex=lineFromFile.indexOf("]");

        String author1=lineFromFile.substring(initialIndex+1);
        String author=author1.replace("]","");
        collector.emit(input,new Values(author));


    }
}
