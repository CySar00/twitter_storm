package storm.bolt.GaussianRankAndMixtureModel.Rank;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 4/3/15.
 */
public class Rank extends BaseBasicBolt {
    Map<String,double[]>CDF=new HashMap<String,double[]>();
    Map<String,Double>rankMap=new HashMap<String, Double>();
    Map<String,Double>rankMap1=new HashMap<String, Double>();

    Map<String,Double>tempCDF0=new HashMap<String, Double>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        CDF = (Map<String, double[]>) input.getValue(0);

        if (!CDF.isEmpty()) {
          /*  double[] keyValuesPerFeature = new double[CDF.keySet().size()];

            double[] keys = new double[CDF.keySet().size()];
            double[] theFeatures = new double[31];

            for (Map.Entry<String, double[]> entry : CDF.entrySet()) {
                String author = entry.getKey();
                double[] vector = entry.getValue();

                for (int i = 0; i < vector.length; i++) {

                    for (int j = 0; j < keyValuesPerFeature.length; j++) {
                        keyValuesPerFeature[j] = vector[i];
                    }

                    double rank = 1;
                    for (int j = 0; j < keyValuesPerFeature.length; j++) {
                        rank *= keyValuesPerFeature[j];

                        rankMap.put(author, rank);
                    }

                    if (rankMap.entrySet().size() == CDF.entrySet().size()) {
                        System.out.println("1 " + rankMap);
                    }
                }


            }
            */
            for(Map.Entry<String,double[]>entry:CDF.entrySet()){
                double rank0=1;
                double rank1=1;
                double rank2=1;
                double rank3=1;
                double rank4=1;
                double rank5=1;
                double rank6=1;
                double rank7=1;
                double rank8=1;
                double rank9=1;
                double rank10=1;
                double rank11=1;
                double rank12=1;
                double rank13=1;
                double rank14=1;
                double rank15=1;
                double rank16=1;
                double rank17=1;
                double rank18=1;
                double rank19=1;
                double rank20=1;
                double rank21=1;
                double rank22=1;
                double rank23=1;
                double rank24=1;
                double rank25=1;
                double rank26=1;
                double rank27=1;
                double rank28=1;
                double rank29=1;
                double rank30=1;


                rank0*=entry.getValue()[0];
                rank1*=entry.getValue()[1];
                rank2*=entry.getValue()[2];
                rank3*=entry.getValue()[3];
                rank4*=entry.getValue()[4];
                rank5*=entry.getValue()[5];
                rank6*=entry.getValue()[6];
                rank7*=entry.getValue()[7];
                rank8*=entry.getValue()[8];
                rank9*=entry.getValue()[9];
                rank10*=entry.getValue()[10];
                rank11*=entry.getValue()[11];
                rank12*=entry.getValue()[12];
                rank13*=entry.getValue()[13];
                rank14*=entry.getValue()[14];
                rank15*=entry.getValue()[15];
                rank16*=entry.getValue()[16];
                rank17*=entry.getValue()[17];
                rank18*=entry.getValue()[18];
                rank19*=entry.getValue()[19];
                rank20*=entry.getValue()[20];
                rank21*=entry.getValue()[21];
                rank22*=entry.getValue()[22];
                rank23*=entry.getValue()[23];
                rank24*=entry.getValue()[24];
                rank25*=entry.getValue()[25];
                rank26*=entry.getValue()[26];
                rank27*=entry.getValue()[27];
                rank28*=entry.getValue()[28];
                rank29*=entry.getValue()[29];
                rank30*=entry.getValue()[30];

                System.out.println(entry.getKey()+" "+rank0+" "+rank1+" "+rank2+" "+rank3+" "+rank4+" "+rank5+" "+rank6+" "+rank7+" "+rank8+" "+rank9+" "
                                                     +rank10+" "+ rank11+" "+rank12+" "+rank13+" "+rank14+" "+rank15+" "+rank16+" "+rank17+" "+rank18+" "+rank19+" "
                                                     +rank20+" "+rank21+" "+rank22+" "+rank22+" "+rank23+" "+rank24+" "+rank25+" "+rank26+" "+rank27+" "+rank28+
                                                    " "+rank29+" "+rank30);
            }

        }
    }
}
