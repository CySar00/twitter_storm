package storm.bolt.GaussianRankAndMixtureModel.MixtureModel;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;

/**
 * Created by christina on 4/3/15.
 */
public class CreateTheMixtureModelDataMap extends BaseBasicBolt {
    public static final int TUPLES=10;

    Map<String,List<Double>>features=new HashMap<String, List<Double>>();
    Map<String,List<Double>>meansK=new HashMap<String, List<Double>>();
    Map<String,List<Double>>sigmaK=new HashMap<String, List<Double>>();
    Map<String,List<Double>>pK=new HashMap<String, List<Double>>();

    Buffer buffer=new CircularFifoBuffer(TUPLES);

    Set<Tuple>tuples=new HashSet<Tuple>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("FEATURES","MEANS_K","SIGMA_K"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(!tuples.contains(input)){
            buffer.add(input);
            tuples.add(input);
        }

        gatherAllDataIntoMultipleMaps();

        if(!features.isEmpty() && !meansK.isEmpty() && !sigmaK.isEmpty() && !pK.isEmpty()){
            collector.emit(new Values(features,meansK,sigmaK));
            System.out.println(features+" "+meansK+" "+sigmaK);
        }
    }

    private void gatherAllDataIntoMultipleMaps(){
        int index=0;

        Iterator iterator=buffer.iterator();
        while (iterator.hasNext()){
            Tuple input=(Tuple)iterator.next();

            if(input!=null){
                String author=input.getString(0);
                double[]features=(double[])input.getValue(1);
                double[]meansK=(double[])input.getValue(2);
                double[]sigmaK=(double[])input.getValue(3);
                double[]pK=(double[])input.getValue(4);

                List<Double>featuresList= Doubles.asList(features);
                List<Double>meansKList=Doubles.asList(meansK);
                List<Double>sigmaKList=Doubles.asList(sigmaK);
                List<Double>pKList=Doubles.asList(pK);

                if(!this.features.containsKey(author)){
                    this.features.put(author,featuresList);
                }

                if(!this.meansK.containsKey(author)){
                    this.meansK.put(author,meansKList);
                }

                if(!this.sigmaK.containsKey(author)){
                    this.sigmaK.put(author,sigmaKList);
                }

                if(!this.pK.containsKey(author)){
                    this.pK.put(author,pKList);
                }
                index+=1;
            }else {
                break;
            }
        }

    }
}
