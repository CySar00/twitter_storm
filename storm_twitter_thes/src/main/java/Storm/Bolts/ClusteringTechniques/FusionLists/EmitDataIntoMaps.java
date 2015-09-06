package Storm.Bolts.ClusteringTechniques.FusionLists;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;

/**
 * Created by christina on 7/30/15.
 */
public class EmitDataIntoMaps extends BaseRichBolt {
    private OutputCollector collector;
    Map<String,List<Double>>map;

    Map<String,double[]>map1;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","MAP"));

    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        map=new HashMap<String, List<Double>>();
        map1=new HashMap<String, double[]>();

    }

    @Override
    public void execute(Tuple input) {
        int clusterIndex=(int)input.getInteger(0);

        String key=input.getString(1);
        List<Double>list=(List<Double>)input.getValue(2);
        double[]vector=Doubles.toArray(list);

        List<Double>mList=map.get(key);
        if(mList==null){
            mList=new ArrayList<Double>();
        }
        mList=list;
        map.put(key,mList);

        double[]mVector=map1.get(key);
        if(mVector==null){
            mVector=new double[vector.length];
        }
        mVector=vector;
        map1.put(key,mVector);


        if(!map1.isEmpty() && !map.isEmpty()) {
        //     System.out.println(clusterIndex + " " + map1);
            collector.emit(input, new Values(clusterIndex, map1));
        }


    }
}
