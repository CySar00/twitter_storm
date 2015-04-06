package storm.bolt.GaussianRankAndMixtureModel.DataMaps;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Created by christina on 4/3/15.
 */
public class ReadDataFromMap extends BaseRichBolt {
    OutputCollector collector;

    Map<String,double[]>fuckingMap=new HashMap<String, double[]>();
    Map<String,String>map1=new HashMap<String, String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("FUCKING_MAP"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lastLineFromFile=input.getString(0);
        Set<String> set=new HashSet<String>(1);


        if(!set.contains(lastLineFromFile)) {
            set.add(lastLineFromFile);

            //System.out.println(lastLineFromFile);
            int firstFuckingIndexOfMap = lastLineFromFile.indexOf("[{");
            int lastFuckingIndexOfMap = lastLineFromFile.indexOf("}]");

            String mapLine = lastLineFromFile.substring(firstFuckingIndexOfMap + 2, lastFuckingIndexOfMap);

            String mapLine1=mapLine.replace("],","]}");
           // System.out.println(mapLine1);

            String[]userAndFeatures=mapLine1.split("}");
            for(int i=0;i<userAndFeatures.length;i++){
             //   System.out.println(userAndFeatures[i]);

                String fuckingAuthor= StringUtils.substringBefore(userAndFeatures[i],"=");
                String fuckingFeatures=StringUtils.substringBetween(userAndFeatures[i],"[","]");

                if(!map1.containsKey(fuckingAuthor)){
                    map1.put(fuckingAuthor,fuckingFeatures);
                }
            }
        }
        //System.out.println(map1);
        for(Map.Entry<String,String>entry1:map1.entrySet()){
            String key=entry1.getKey();
            String value=entry1.getValue();

            String[] values=value.split(",");

            List<Double>aList=new ArrayList<Double>();
            for(int i=0;i<values.length;i++){
                aList.add(Double.parseDouble(values[i]));
            }
            double[]aVector=Doubles.toArray(aList);

            if(!fuckingMap.containsKey(key)){
                fuckingMap.put(key,aVector);
            }

        }
        collector.emit(new Values(fuckingMap));
     //   System.out.println(fuckingMap);
    }
}
