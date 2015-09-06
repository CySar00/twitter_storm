package storm.bolt.GaussianRankAndMixtureModel.Rank;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.special.Erf;


/**
 * Created by christina on 4/3/15.
 */
public class ComputeGaussianCDF extends BaseBasicBolt {
    Map<String,double[]>features=new HashMap<String, double[]>();
    Map<String,double[]>means=new HashMap<String, double[]>();
    Map<String,double[]>sigma=new HashMap<String, double[]>();

    Map<String,double[]>CDFWithVector=new HashMap<String, double[]>();
    Map<String,List<Double>>CDF=new HashMap<String, List<Double>>();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MAP"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        features=(Map<String,double[]>)input.getValue(0);
        means=(Map<String,double[]>)input.getValue(1);
        sigma=(Map<String,double[]>)input.getValue(2);


        for(Map.Entry<String,double[]>featuresEntry:features.entrySet()){
            String anAuthor=featuresEntry.getKey();
            double[]theFeaturesVector=featuresEntry.getValue();

            double[]CDFVector=new double[theFeaturesVector.length];

            if(means.containsKey(anAuthor) && sigma.containsKey(anAuthor)){
                double[]theMeansVector=means.get(anAuthor);
                double[]theSigmaVector=sigma.get(anAuthor);

                for(int i=0;i<theFeaturesVector.length;i++){

                    if(theSigmaVector[i]==0){
                        CDFVector[i]=0.5*(1+org.apache.commons.math3.special.Erf.erf(theFeaturesVector[i]));
                    }else{
                        CDFVector[i]=0.5*(1+org.apache.commons.math3.special.Erf.erf((theFeaturesVector[i]-theMeansVector[i])/(theSigmaVector[i]*Math.sqrt(2))));
                    }

                    
                }
                if(!CDFWithVector.containsKey(anAuthor)){
                    CDFWithVector.put(anAuthor,CDFVector);
                }
            }
        }

        for(Map.Entry<String,double[]>entry:CDFWithVector.entrySet()){
            String author=entry.getKey();
            double[]cdfVectorValues=entry.getValue();
            List<Double>cdfList= Doubles.asList(cdfVectorValues);
            CDF.put(author,cdfList);
        }
        collector.emit(new Values(CDFWithVector));


    }


}
