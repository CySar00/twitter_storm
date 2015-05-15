package storm.bolt.Clustering.FuzzyClustering.classify;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 4/28/15.
 */
public class FuzzyClusterIndexEquals0  extends BaseBasicBolt {
    Map<String, double[]> authorAndFeatures = new HashMap<String, double[]>();
    Map<String, List<Double>> authorAndFeaturesAsList = new HashMap<String, List<Double>>();
    Set<String> theAuthors = new HashSet<String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX", "AUTHOR", "FEATURES_AS_LIST"));
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        int clusterIndex = (int) input.getInteger(0);
        String author = input.getString(1);
        double[] featuresAsVector = (double[]) input.getValue(2);

        System.out.println(clusterIndex + ": " +author + " " + featuresAsVector);

        double[] vector = authorAndFeatures.get(author);
        if (vector == null) {
            vector = new double[featuresAsVector.length];
        }
        vector = featuresAsVector;

        List<Double> featuresAsList = authorAndFeaturesAsList.get(author);
        if (featuresAsList == null) {
            featuresAsList = new ArrayList<Double>();
        }

        if (clusterIndex == 0) {
            if (!theAuthors.contains(author)) {

                for (int i = 0; i < vector.length; i++) {
                    featuresAsList.add(vector[i]);
                }
                collector.emit(new Values(1, author, featuresAsList));
                theAuthors.add(author);
            }
        }

    }
}
