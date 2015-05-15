package storm.bolt.FusionLists.Functions;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 4/29/15.
 */
public class FusionLists {

    public static void fusionLists(Map<String,double[]> ranks,BasicOutputCollector  collector){
        RankAggregation<String>rankAggregation=new RankAggregation<String>();
        Map<String,Map<String,Double>>scores=new HashMap<String, Map<String, Double>>();

        for(String key:ranks.keySet()){
            scores.put(key,new HashMap<String, Double>());
            String temp=null;

            for(int i=0;i<ranks.get(key).length;i++){
                temp="Metric".concat(String.valueOf(i));
                scores.get(key).put(temp,ranks.get(key)[i]);
            }
        }
        System.out.println("fucking scores : " + scores);
        Map<String,Double>resultReciprocalRank=rankAggregation.reciprocalRankFusion(scores);
        System.out.println("fucking result of the reciprocal rank aggregation : "+resultReciprocalRank);

        if(resultReciprocalRank!=null || !resultReciprocalRank.isEmpty()){
            collector.emit(new Values(resultReciprocalRank));
        }
    }
}
