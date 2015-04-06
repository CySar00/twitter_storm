package storm.bolt.Clustering.KClustering.Classify;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.*;

/**
 * Created by christina on 4/1/15.
 */
public class DataPointsWithClusterIndexEquals1 extends BaseBasicBolt {



    Map<String, double[]> authorAndVectorMap = new HashMap<String, double[]>();
    Map<String, List<Double>> authorAndListMap = new HashMap<String, List<Double>>();
    Set<String>theFuckingAuthors=new HashSet<String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("CLUSTER_INDEX","FUCKING_AUTHOR", "FUCKING_LIST"));

    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        int clusterIndex = (int) input.getInteger(0);
        String aFuckingAuthor = input.getString(1);
        double[] vector = (double[]) input.getValue(2);



        double[] vector1 = authorAndVectorMap.get(aFuckingAuthor);
        if (vector1 == null)
            vector1 = new double[vector.length];
        vector1 = vector;

        List<Double> list = authorAndListMap.get(aFuckingAuthor);
        if (list == null) {
            list = new ArrayList<Double>();
        }

        if (clusterIndex == 1) {
            if(!theFuckingAuthors.contains(aFuckingAuthor)) {


                for (int i = 0; i < vector.length; i++) {
                    list.add((vector[i]));
                }
                System.out.println(aFuckingAuthor + " " + list);

                collector.emit(new Values(1,aFuckingAuthor,list));
                theFuckingAuthors.add(aFuckingAuthor);
            }

        }
    }
}
