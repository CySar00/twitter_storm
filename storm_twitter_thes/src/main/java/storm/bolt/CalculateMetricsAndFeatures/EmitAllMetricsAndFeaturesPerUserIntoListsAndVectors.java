package storm.bolt.CalculateMetricsAndFeatures;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 3/28/15.
 */
public class EmitAllMetricsAndFeaturesPerUserIntoListsAndVectors extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<Double>>userAndListOfMetricsAndFeatures=new HashMap<String, List<Double>>();
    Map<String,double[]>userAndVectorOfMetricsAndFeatures=new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","LIST"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);

        List<Double>metricsAndFeaturesList=userAndListOfMetricsAndFeatures.get(author);
        if(metricsAndFeaturesList==null)
            metricsAndFeaturesList=new ArrayList<Double>();

        double[]metricsAndFeaturesVector=userAndVectorOfMetricsAndFeatures.get(author);
        if(metricsAndFeaturesVector==null)
            metricsAndFeaturesVector=new double[input.getFields().size()-1];

        for(int i=1;i<input.getFields().size();i++){
            metricsAndFeaturesList.add(input.getDouble(i));
        }

        metricsAndFeaturesVector= Doubles.toArray(metricsAndFeaturesList);

        if(!userAndListOfMetricsAndFeatures.containsKey(author) && !userAndVectorOfMetricsAndFeatures.containsKey(author)){
            collector.emit(new Values(author,metricsAndFeaturesList));


            userAndListOfMetricsAndFeatures.put(author,metricsAndFeaturesList);
            userAndVectorOfMetricsAndFeatures.put(author,metricsAndFeaturesVector);

        }

    }
}
