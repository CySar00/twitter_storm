package storm.bolt.GaussianRankAndMixtureModel.MixtureModel;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.bolt.GaussianRankAndMixtureModel.Functions.CalculateTheMeansAndCovarianceForAVector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 4/3/15.
 */
public class CalculateTheInitialMeansAndCovarianceForEachFeature extends BaseBasicBolt{
    Random random=new Random();

    Map<String,double[]>features=new HashMap<String, double[]>();
    Map<String,double[]>means=new HashMap<String, double[]>();
    Map<String,double[]>covariance=new HashMap<String, double[]>();
    Map<String,double[]>mixtureCoefficients=new HashMap<String, double[]>();



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","FEATURES","MEANS","COVARIANCE","MIXTURE_COEFFICIENTS"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.features=(Map<String,double[]>)input.getValue(0);

        if(!this.features.isEmpty()){
            double[]keyValuesPerFeature=new double[this.features.keySet().size()];

            double[]means=new double[31];
            double[]covariance=new double[31];
            double[]mixtureCoefficients=new double[31];


            for(int i=0;i<31;i++){
                int keyIndex=0;
                for(Map.Entry<String,double[]>entry:this.features.entrySet()){
                    keyValuesPerFeature[keyIndex]=entry.getValue()[i];
                    keyIndex+=1;
                }
                means[i]=CalculateTheMeansAndCovarianceForAVector.calculateAVectorsMeansValue(keyValuesPerFeature);
                covariance[i]=CalculateTheMeansAndCovarianceForAVector.calculateAVectorsCovariance(keyValuesPerFeature);
                mixtureCoefficients[i]=0.01+random.nextDouble();
            }

            for(String key:this.features.keySet()){

                if(!this.means.containsKey(key) && !this.covariance.containsKey(key) && !this.mixtureCoefficients.containsKey(key)){

                    collector.emit(new Values(key,this.features.get(key),this.means.get(key),this.covariance.get(key),this.mixtureCoefficients.get(key)));

                    this.means.put(key,means);
                    this.covariance.put(key,covariance);
                    this.mixtureCoefficients.put(key,mixtureCoefficients);
                }

            }





           /* for(Map.Entry<String,double[]>entry:this.features.entrySet()){
                String fuckingAuthor=entry.getKey();
                double[]features=entry.getValue();

                double[]means=new double[features.length];
                double[]covariance=new  double[features.length];
                double[]mixtureCoefficients=new double[features.length];

                for(int i=0;i<features.length;i++){
                    mixtureCoefficients[i]=0.01+random.nextDouble();

                    for(int j=0;j<keyValuesPerFeature.length;j++){
                        keyValuesPerFeature[j]=features[i];
                    }

                    means[i]= CalculateTheMeansAndCovarianceForAVector.calculateAVectorsMeansValue(keyValuesPerFeature);
                    covariance[i]=CalculateTheMeansAndCovarianceForAVector.calculateAVectorsCovariance(keyValuesPerFeature);
                }

                if(!this.means.containsKey(fuckingAuthor) && !this.covariance.containsKey(fuckingAuthor) && !this.mixtureCoefficients.containsKey(fuckingAuthor)){
                    collector.emit(new Values(fuckingAuthor,features,means,covariance,mixtureCoefficients));

                    this.means.put(fuckingAuthor,means);
                    this.covariance.put(fuckingAuthor,covariance);
                    this.mixtureCoefficients.put(fuckingAuthor,mixtureCoefficients);
                }

            }*/
        }
    }
}
