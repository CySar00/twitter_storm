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
public class ReCalculateTheMixtureModelCoefficients extends BaseBasicBolt {
    Map<String, double[]> features = new HashMap<String, double[]>();
    Map<String, double[]> posteriorProbability = new HashMap<String, double[]>();
    Map<String, double[]> Nk = new HashMap<String, double[]>();
    Map<String, double[]> meansK = new HashMap<String, double[]>();
    Map<String, double[]> sigmaK = new HashMap<String, double[]>();
    Map<String, double[]> pK = new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR", "FEATURES", "POSTERIOR_PROBABILITY", "N_K", "MEANS_K", "SIGMA_K", "P_K"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String author = input.getString(0);
        double[] features = (double[]) input.getValue(1);
        double[] posteriorProbability = (double[]) input.getValue(2);
        double[] Nk = (double[]) input.getValue(3);

        double[] meansK = (double[]) input.getValue(4);

        double[] sigmaK = (double[]) input.getValue(5);

        double[] pK = new double[features.length];

        for (int i = 0; i < pK.length; i++) {
            pK[i] = Nk[i] / features.length;
        }

        if (!this.features.containsKey(author) && !this.posteriorProbability.containsKey(author) && !this.Nk.containsKey(author) && !this.meansK.containsKey(author) && !this.sigmaK.containsValue(author) && !this.pK.containsKey(author)) {
            collector.emit(new Values(author, features, posteriorProbability, Nk, meansK, sigmaK, pK));

            this.features.put(author, features);
            this.posteriorProbability.put(author, posteriorProbability);
            this.Nk.put(author, Nk);
            this.meansK.put(author, meansK);
            this.sigmaK.put(author, sigmaK);
            this.pK.put(author, pK);
        }

    }
}