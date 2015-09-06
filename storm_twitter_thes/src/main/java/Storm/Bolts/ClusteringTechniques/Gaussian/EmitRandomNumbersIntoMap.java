package Storm.Bolts.ClusteringTechniques.Gaussian;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/21/15.
 */
public class EmitRandomNumbersIntoMap extends BaseBasicBolt {
    Buffer buffer=new CircularFifoBuffer();
    Map<String,List<Double>>map=new HashMap<String, List<Double>>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MAP"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        buffer.add(input);
        create();
        collector.emit(new Values(map));

    }

    private void create(){
        Iterator iterator=buffer.iterator();
        int index=0;
        while (iterator.hasNext()){
            Tuple tuple=(Tuple)iterator.next();

            List<Double>list=(List<Double>)tuple.getValue(0);
            map.put(String.valueOf(index),list);
            index+=1;
        }
        System.out.println(map+" "+map.size());
    }
}
