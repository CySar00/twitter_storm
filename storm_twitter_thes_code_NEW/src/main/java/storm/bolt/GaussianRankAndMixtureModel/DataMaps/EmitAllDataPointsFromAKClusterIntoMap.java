package storm.bolt.GaussianRankAndMixtureModel.DataMaps;

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
 * Created by christina on 4/2/15.
 */
public class EmitAllDataPointsFromAKClusterIntoMap extends BaseBasicBolt{
    public static final int TUPLES=10;
    Buffer buffer=new CircularFifoBuffer(TUPLES);

    Map<String,List<Double>>map=new HashMap<String, List<Double>>();

    int largestIndex;

    Random random=new Random();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MAP"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Set<Tuple>tuples=new HashSet<Tuple>();
        if(!tuples.contains(input)){
            buffer.add(input);
            tuples.add(input);
        }

        emitAllDataIntoMap();
        if(!map.isEmpty()){
            System.out.println(map);
            collector.emit(new Values(map));
        }


    }

    private void emitAllDataIntoMap(){
        int index=0;
        final int largestNumber;
        Map<String,List<Double>>map=new HashMap<String, List<Double>>();

        Iterator iterator=buffer.iterator();
        while (iterator.hasNext()) {
            Tuple input = (Tuple) iterator.next();

            if (input != null) {
                String author = input.getString(0);
                List<Double>list =(List<Double>)input.getValue(1);

                if(!map.containsKey(author)){
                    map.put(author,list);
                }

                index += 1;

            }else{
                break;
            }
        }
        largestIndex=index;

        for(Map.Entry<String,List<Double>>entry:map.entrySet()){
            if(!this.map.containsKey(entry.getKey())){
                this.map.put(entry.getKey(),entry.getValue());

            }
        }
    }
}
