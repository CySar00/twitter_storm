package storm.bolt.GaussianRankAndMixtureModel.MixtureModel;

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
public class CalculateGaussianDistribution extends BaseBasicBolt {
    Map<String,double[]>gaussianDistribution=new HashMap<String, double[]>();
    Map<String,double[]>features=new HashMap<String, double[]>();
    Map<String,double[]>coefficients=new HashMap<String, double[]>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","FEATURES","COEFFICIENTS","DISTRIBUTION"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String author=input.getString(0);
        double[]features=(double[])input.getValue(1);
        double[]means=(double[])input.getValue(2);
        double[]covariance=(double[])input.getValue(3);
        double[]coefficients=(double[])input.getValue(4);

        double[]gaussianDistribution=new double[features.length];

        for(int i=0;i<features.length;i++){

            if(covariance[i]==0){
                gaussianDistribution[i]=(1/Math.sqrt(2*Math.PI))*Math.exp(-(features[i]-means[i]*features[i]-means[i]));
            }else {
                gaussianDistribution[i]=(1/(covariance[i]*Math.sqrt(2*Math.PI)))*Math.exp((-(features[i]-means[i]*features[i]-means[i]))/(2*covariance[i]*covariance[i]));
            }
        }

        if(!this.features.containsKey(author) && !this.coefficients.containsKey(author) && !this.gaussianDistribution.containsKey(author)){
            collector.emit(new Values(author,features,coefficients,gaussianDistribution));

            this.features.put(author,features);
            this.coefficients.put(author,coefficients);
            this.gaussianDistribution.put(author,gaussianDistribution);
        }
    }
}
