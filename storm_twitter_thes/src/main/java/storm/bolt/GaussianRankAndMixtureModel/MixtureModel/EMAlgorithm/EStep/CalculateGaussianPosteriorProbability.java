package storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.EStep;

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
public class CalculateGaussianPosteriorProbability extends BaseBasicBolt {
    Map<String,double[]>features=new HashMap<String, double[]>();
    Map<String,double[]>posteriorProbability=new HashMap<String, double[]>();
    Map<String,double[]>normalizedPosteriorProbability=new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","FEATURES","POSTERIOR_PROBABILITY"));


    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String author=input.getString(0);
        double[]features=(double[])input.getValue(1);
        double[]coefficients=(double[])input.getValue(2);
        double[]gaussianDistribution=(double[])input.getValue(3);

        double[]posteriorProbability=new double[features.length];
        double[]normalizedPosteriorProbability=new double[features.length];
        double sum=0;

        for(int i=0;i<features.length;i++){
            posteriorProbability[i]=gaussianDistribution[i]*coefficients[i];
            sum+=posteriorProbability[i];
        }

        for(int i=0;i<features.length;i++){
            normalizedPosteriorProbability[i]=posteriorProbability[i]/sum;
        }

        if(!this.features.containsKey(author) && !this.posteriorProbability.containsKey(author) && !this.normalizedPosteriorProbability.containsKey(author)){
            System.out.println(author+" "+features+" "+normalizedPosteriorProbability);

            collector.emit(new Values(author,features,normalizedPosteriorProbability));

            this.features.put(author,features);
            this.posteriorProbability.put(author,posteriorProbability);
            this.normalizedPosteriorProbability.put(author,normalizedPosteriorProbability);
        }

    }
}
