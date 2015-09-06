package Storm.Bolts.ClusteringTechniques.Gaussian.GMM.InitialValues;


import Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Parallel_Like.Functions.MixtureModeling;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.*;

/**
 * Created by christina on 7/21/15.
 */
public class EmitMap extends BaseRichBolt {
    private  OutputCollector collector;
    Map<String,List<Double>>map;
    Map<String,double[]>map1;

    Random random=new Random();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MAP"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        map=new HashMap<String, List<Double>>();
        map1=new HashMap<String, double[]>();
    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);
        System.out.println(line);

        int firstIndex=line.indexOf("[{");
        int lastIndex=line.indexOf("]}]");

        int index=0;
        String subStringFromLine=line.substring(firstIndex+2,lastIndex);
        String[]firstSplit=subStringFromLine.split("],");
        for(int i=0;i<firstSplit.length;i++){
            String[]secondSplit=firstSplit[i].split("=");
            String author=secondSplit[0];

            String subSecondSplit=secondSplit[1].replace("[","");
            String []subThirdSplit=subSecondSplit.split(",");

            List<Double>list=new ArrayList<Double>();
            for(int j=0;j<subThirdSplit.length;j++){
                list.add(Double.valueOf(subThirdSplit[j]));
            }
            map.put(author,list);
            map1.put(author, Doubles.toArray(list));
        }


        MixtureModeling.MixtureModeling(map1,collector,32);







      //  collector.emit(input,new Values(map1));
    }



}
