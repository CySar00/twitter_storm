package storm.bolt.FusionLists.ReadData;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;

/**
 * Created by christina on 4/29/15.
 */
public class EmitAuthorAndListOfFeaturesIntoMap extends BaseBasicBolt {
    public static final int TUPLES=10;

    Buffer buffer=new CircularFifoBuffer(TUPLES);
    Map<String,List<Double>>map=new HashMap<String, List<Double>>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MAP"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Set<Tuple> set=new HashSet<Tuple>();
        if(!set.contains(input)){
            buffer.add(input);
            set.add(input);
        }
        emitAuthorAndListOfFeaturesIntoMap();
        if(!map.isEmpty()){
            collector.emit(new Values(map));
        }

    }

    private void emitAuthorAndListOfFeaturesIntoMap(){
        int index=0;
        Set<String>set=new HashSet<String>();
        Iterator iterator=buffer.iterator();
        while (iterator.hasNext()){
            Tuple input=(Tuple)iterator.next();

            if(input!=null){
                String author=input.getString(0);
                List<Double>theFeatures=(List<Double>)input.getValue(1);

                if(author!=null && theFeatures!=null){
                    if(!map.containsKey(author) && !set.contains(author)){
                        map.put(author,theFeatures);
                        set.add(author);
                    }
                }
            }
        }


    }
}
