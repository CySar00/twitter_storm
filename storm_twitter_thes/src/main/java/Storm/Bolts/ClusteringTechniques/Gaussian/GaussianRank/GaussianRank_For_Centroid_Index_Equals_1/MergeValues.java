package Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank.GaussianRank_For_Centroid_Index_Equals_1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/20/15.
 */
public class MergeValues extends BaseRichBolt {
    private OutputCollector collector;

    String author1;String author2;
    List<Double>values;

    List<Double>means;List<Double>sigma;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;


    }

    @Override
    public void execute(Tuple input) {
        final String sourceComponent=input.getSourceComponent();

        if(sourceComponent.equals("PROCESS_AUTHOR_AND_FEATURES_FROM_CENTROID_IS_1_TEXT_FILE")){
            author1=input.getString(0);
            values=(List<Double>)input.getValue(1);
        }

        if(sourceComponent.equals("PROCESS_AUTHORS_AND_EM_VALUES_FOR_FURTHER_PROCESSING")){
            author2=input.getString(0);
            means=(List<Double>)input.getValue(1);
            sigma=(List<Double>)input.getValue(2);
        }


    }
}
