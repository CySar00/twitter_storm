package Storm.Bolts.Preprocessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.io.Output;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by christina on 7/16/15.
 */
public class RemoveDuplicateIDsFromMongoDB extends BaseRichBolt {
    Set<Long>IDs;
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","ID","TWEET","DATE","IN_REPLY_TO"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        IDs=new HashSet<Long>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        Long ID=input.getLong(1);
        String tweet=input.getString(2);
        Date date=(Date)input.getValue(3);
        Long inReplyTo=input.getLong(4);

        if(!IDs.contains(ID)){
            collector.emit(input,new Values(author,ID,tweet,date,inReplyTo));
            IDs.add(ID);
        }

    }
}
