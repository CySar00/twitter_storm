package storm.bolt.FusionLists;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.bolt.FusionLists.Functions.FusionLists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 5/5/15.
 */
public class CreateFusionListsForClusterIndexEquals2 extends BaseBasicBolt {
    Map<String,List<Double>> map=new HashMap<String, List<Double>>();
    Map<String,double[]>valuesConvertedToVectorMap=new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("FUSION_LISTS"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        map=(Map<String,List<Double>>)input.getValue(0);

        if(!map.isEmpty() || map!=null){
            for(Map.Entry<String,List<Double>>entry:map.entrySet()){
                String key=entry.getKey();
                List<Double>list=entry.getValue();
                double[]vectorConverted=new double[list.size()];

                if(list!=null || !list.isEmpty()){

                    for(int i=0;i<vectorConverted.length;i++){
                        vectorConverted[i]=list.get(i);
                    }
                }

                if(key!=null && vectorConverted!=null){
                    valuesConvertedToVectorMap.put(key,vectorConverted);
                }

            }

            if(!valuesConvertedToVectorMap.isEmpty()){
                FusionLists.fusionLists(valuesConvertedToVectorMap, collector);
            }
        }

    }

}

