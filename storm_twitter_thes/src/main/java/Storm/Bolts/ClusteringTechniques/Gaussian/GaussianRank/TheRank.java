package Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.nio.Buffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by christina on 7/26/15.
 */
public class TheRank extends BaseBasicBolt{
    org.apache.commons.collections.Buffer buffer=new CircularFifoBuffer();

    Map<String,Double>feature0=new HashMap<String, Double>();
    Map<String,Double>feature1=new HashMap<String, Double>();
    Map<String,Double>feature2=new HashMap<String, Double>();
    Map<String,Double>feature3=new HashMap<String, Double>();
    Map<String,Double>feature4=new HashMap<String, Double>();
    Map<String,Double>feature5=new HashMap<String, Double>();
    Map<String,Double>feature6=new HashMap<String, Double>();;
    Map<String,Double>feature7=new HashMap<String, Double>();
    Map<String,Double>feature8=new HashMap<String, Double>();
    Map<String,Double>feature9=new HashMap<String, Double>();
    Map<String,Double>feature10=new HashMap<String, Double>();
    Map<String,Double>feature11=new HashMap<String, Double>();
    Map<String,Double>feature12=new HashMap<String, Double>();
    Map<String,Double>feature13=new HashMap<String, Double>();
    Map<String,Double>feature14=new HashMap<String, Double>();
    Map<String,Double>feature15=new HashMap<String, Double>();
    Map<String,Double>feature16=new HashMap<String, Double>();
    Map<String,Double>feature17=new HashMap<String, Double>();
    Map<String,Double>feature18=new HashMap<String, Double>();
    Map<String,Double>feature19=new HashMap<String, Double>();
    Map<String,Double>feature20=new HashMap<String, Double>();
    Map<String,Double>feature21=new HashMap<String, Double>();
    Map<String,Double>feature22=new HashMap<String, Double>();
    Map<String,Double>feature23=new HashMap<String, Double>();
    Map<String,Double>feature24=new HashMap<String, Double>();
    Map<String,Double>feature25=new HashMap<String, Double>();
    Map<String,Double>feature26=new HashMap<String, Double>();
    Map<String,Double>feature27=new HashMap<String, Double>();
    Map<String,Double>feature28=new HashMap<String, Double>();
    Map<String,Double>feature29=new HashMap<String, Double>();
    Map<String,Double>feature30=new HashMap<String, Double>();
    Map<String,Double>feature31=new HashMap<String, Double>();



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        buffer.add(input);
        rank();

        for(String key:feature0.keySet()){
            collector.emit(new Values(key,feature0.get(key),
                    feature1.get(key),feature2.get(key),feature3.get(key),feature4.get(key),feature5.get(key),
                    feature6.get(key),feature7.get(key),feature8.get(key),feature9.get(key),feature10.get(key),feature11.get(key),
                    feature12.get(key),feature13.get(key),feature14.get(key),feature15.get(key),feature16.get(key),feature17.get(key),
                    feature18.get(key),feature19.get(key),feature20.get(key),feature21.get(key),feature22.get(key),feature23.get(key),feature24.get(key),
                    feature25.get(key),feature26.get(key),feature27.get(key),feature28.get(key),feature29.get(key),feature30.get(key),
                    feature31.get(key)
                    ));

        }



    }

    private void rank(){
        Iterator iterator=buffer.iterator();
        double rank0=1;double rank1=1;double rank2=1;double rank3=1;
        double rank4=1;double rank5=1;double rank6=1;double rank7=1;
        double rank8=1;double rank9=1;double rank10=1;double rank11=1;
        double rank12=1;double rank13=1;double rank14=1;double rank15=1;
        double rank16=1;double rank17=1;double rank18=1;double rank19=1;
        double rank20=1;double rank21=1;double rank22=1;double rank23=1;
        double rank24=1;double rank25=1;double rank26=1;double rank27=1;
        double rank28=1;double rank29=1;double rank30=1;double rank31=1;

        while (iterator.hasNext()){
            Tuple tuple=(Tuple)iterator.next();

            String author=tuple.getString(0);
            double[]cdfs=(double[])tuple.getValue(2);

            rank0*=cdfs[0];
            rank1*=cdfs[1];
            rank2*=cdfs[2];
            rank3*=cdfs[3];
            rank4*=cdfs[4];
            rank5*=cdfs[5];
            rank6*=cdfs[6];
            rank7*=cdfs[7];
            rank8*=cdfs[8];
            rank9*=cdfs[9];
            rank10*=cdfs[10];
            rank11*=cdfs[11];
            rank12*=cdfs[12];
            rank13*=cdfs[13];
            rank14*=cdfs[14];
            rank15*=cdfs[15];
            rank16*=cdfs[16];
            rank17*=cdfs[17];
            rank18*=cdfs[18];
            rank19*=cdfs[19];
            rank20*=cdfs[20];
            rank21*=cdfs[21];
            rank22*=cdfs[22];
            rank23*=cdfs[23];
            rank24*=cdfs[24];
            rank25*=cdfs[25];
            rank26*=cdfs[26];
            rank27*=cdfs[27];
            rank28*=cdfs[28];
            rank29*=cdfs[29];
            rank30*=cdfs[30];
            rank31*=cdfs[31];

            feature0.put(author,rank0);
            feature1.put(author,rank1);
            feature2.put(author,rank2);
            feature3.put(author,rank3);
            feature4.put(author,rank4);
            feature5.put(author,rank5);
            feature6.put(author,rank6);
            feature7.put(author,rank7);
            feature8.put(author,rank8);
            feature9.put(author,rank9);
            feature10.put(author,rank10);
            feature11.put(author,rank11);
            feature12.put(author,rank12);
            feature13.put(author,rank13);
            feature14.put(author,rank14);
            feature15.put(author,rank15);
            feature16.put(author,rank16);
            feature17.put(author,rank17);
            feature18.put(author,rank18);
            feature19.put(author,rank19);
            feature20.put(author,rank20);
            feature21.put(author,rank21);
            feature22.put(author,rank22);
            feature23.put(author,rank23);
            feature24.put(author,rank24);
            feature25.put(author,rank25);
            feature26.put(author,rank26);
            feature27.put(author,rank27);
            feature28.put(author,rank28);
            feature29.put(author,rank29);
            feature30.put(author,rank30);
            feature31.put(author,rank31);

        }

        for(String key:feature0.keySet()){
            System.out.println(key+" "+feature0.get(key));
        }
    }
}

