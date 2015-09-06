package storm.bolt.Clustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import org.apache.commons.lang.StringUtils;
import sun.net.idn.StringPrep;

import java.util.*;

/**
 * Created by christina on 3/28/15.
 */
public class CreatingTheDataSet extends BaseRichBolt {
    private OutputCollector collector;

    HashMap<String,double[]>map=new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","VECTOR"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        //System.out.println(line);

        String temp=line.replace(":source: EMIT_ALL_METRICS_AND_FEATURES_INTO_LIST_AND_VECTOR:581, stream: default, id: {},",":");
        String[]temps=temp.split(":");

        String tupleStringValues=temps[temps.length-1];
        String temp1=tupleStringValues.substring(4,tupleStringValues.length());
        String temp2=temp1.substring(1);

        String temp3=temp2.replace(", [", " [");
    //    System.out.println(temp3);
        String[]temp4=temp3.split("\\[");
//        System.out.println(temp4[1]);

        String author=StringUtils.substringBefore(temp3,"[");
        String values=StringUtils.substringBetween(temp3,"[","]]");
        //System.out.println(author);
        //System.out.println(values);

        String values1=values.replace("["," ");

       // System.out.println(values1);

        String[]aValue=values1.split(",");
        final ArrayList<Double>valuesAsDoubleList=new ArrayList<Double>();
        for(int i=1;i<aValue.length;i++){

            valuesAsDoubleList.add(Double.parseDouble(aValue[i]));
        }
     //   System.out.println(valuesAsDoubleList);
        double[]valuesAsDoubleVector= Doubles.toArray(valuesAsDoubleList);

        if(!map.containsKey(author)) {

            map.put(author,valuesAsDoubleVector);
           System.out.println(author+" "+valuesAsDoubleVector);
            collector.emit(input, new Values(author, valuesAsDoubleVector));
        }
       // collector.ack(input);
    }
}
