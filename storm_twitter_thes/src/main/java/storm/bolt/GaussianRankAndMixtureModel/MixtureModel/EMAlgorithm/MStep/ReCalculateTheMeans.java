package storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 4/3/15.
 */
public class ReCalculateTheMeans extends BaseBasicBolt {
    Map<String, double[]> features = new HashMap<String, double[]>();
    Map<String, double[]> posteriorProbability = new HashMap<String, double[]>();
    Map<String, double[]> Nk = new HashMap<String, double[]>();
    Map<String,double[]>meansK=new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR", "FEATURES", "POSTERIOR_PROBABILITY", "N_K","MEANS_K"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String author = input.getString(0);
        double[] features = (double[]) input.getValue(1);
        double[] posteriorProbability = (double[]) input.getValue(2);
        double[] Nk = (double[])input.getValue(3);

        double[]meansK=new double[features.length];
        for(int i=0;i<features.length;i++){
            meansK[i]+=features[i]*posteriorProbability[i];
        }

        for(int i=0;i<meansK.length;i++){
            meansK[i]=meansK[i]/Nk[i];
        }


        if (!this.features.containsKey(author) && !this.posteriorProbability.containsKey(author) && !this.Nk.containsKey(author) && !this.meansK.containsKey(author)) {
            collector.emit(new Values(author,features, posteriorProbability, Nk,meansK));

            this.features.put(author, features);
            this.posteriorProbability.put(author, posteriorProbability);
            this.Nk.put(author, Nk);
            this.meansK.put(author,meansK);
        }

    }
}
