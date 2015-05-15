package storm.bolt.Clustering.FuzzyClustering;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import storm.bolt.Clustering.Functions.SerializeAndDeserializeJavaObjects;
import storm.bolt.Databases.Cassandra.FuzzyClustersDatabase;

import java.util.*;

/**
 * Created by christina on 4/21/15.
 */
public class CreateTheInitialFuzzyCentroids extends BaseBasicBolt {
    public static final int TUPLES=10;
    public static final int CLUSTERS=3;

    Buffer buffer=new CircularFifoBuffer(TUPLES);

    Map<Integer,List<Double>>map=new HashMap<Integer, List<Double>>();
    Map<Integer,List<Double>>centroids=new HashMap<Integer, List<Double>>();

    String serialized="";

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Set<Tuple>set=new HashSet<Tuple>();
        if(!set.contains(input)) {
            buffer.add(input);
            set.add(input);
        }
        createTheFuzzyCentroids();

    }

    private void createTheFuzzyCentroids(){
        int index=0;

        Iterator iterator=buffer.iterator();
        Set<double[]>uniqueVectors=new HashSet<double[]>();
        Set<String>uniqueAuthors=new HashSet<String>();

        Set<List<Double>>uniqueSetOfListsOfFeatures=new HashSet<List<Double>>();
        List<List<Double>>listsOfFeatures=new ArrayList<List<Double>>();

        while (iterator.hasNext()){
            Tuple input=(Tuple)iterator.next();

            String author=input.getString(0);
            double[]theAuthorsFeatures=(double[])input.getValue(1);

            List<Double>listOfFeatures=new ArrayList<Double>();

            if(theAuthorsFeatures!=null){
                for(int i=0;i<theAuthorsFeatures.length;i++){
                    listOfFeatures.add(theAuthorsFeatures[i]);
                }
            }

            if(!uniqueSetOfListsOfFeatures.contains(listOfFeatures)){
                listsOfFeatures.add(listOfFeatures);
                uniqueSetOfListsOfFeatures.add(listOfFeatures);
            }

        }

        if(listsOfFeatures.size()==CLUSTERS) {
         //   System.out.println(fuckingListOfFuckingFeatures);
            Iterator iterator1=listsOfFeatures.iterator();
            while (iterator1.hasNext()) {
                List<Double> aList = (List<Double>) iterator1.next();

                map.put(index, aList);
                index += 1;
            }
        }

        if(!map.isEmpty() && map.entrySet().size()==CLUSTERS){
           // System.out.println(map);

            for(Map.Entry<Integer,List<Double>>entry:map.entrySet()){
                int clusterIndex=(int)entry.getKey();
                List<Double>listOfFeatures=(List<Double>)entry.getValue();

                String[]strings=new String[CLUSTERS];
                serialized=SerializeAndDeserializeJavaObjects.convertDoubleListToString(listOfFeatures);
                //System.out.println(serialized);
                if(serialized!=null){
                    FuzzyClustersDatabase.setSerializedMap(clusterIndex,serialized);
                    strings[clusterIndex]=serialized;
                }

            }

        }
    }
}
