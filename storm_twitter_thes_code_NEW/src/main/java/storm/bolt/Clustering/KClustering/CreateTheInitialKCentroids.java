package storm.bolt.Clustering.KClustering;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import storm.bolt.Clustering.Functions.SerializeAndDeserializeJavaObjects;
import storm.bolt.Databases.Cassandra.KClustersDatabase;

import java.io.IOException;
import java.util.*;

/**
 * Created by christina on 4/1/15.
 */
public class CreateTheInitialKCentroids extends BaseBasicBolt {
    public static final int TUPLES=10;
    public static final int CLUSTERS=2;

    Map<Integer,List<Double>>map=new HashMap<Integer,List<Double>>();
    Map<Integer,double[]>centroids=new HashMap<Integer, double[]>();

    String serializedVectors="";
    Buffer buffer=new CircularFifoBuffer(TUPLES);




    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        HashSet<Tuple>tuples=new HashSet<Tuple>();
        if(!tuples.contains(input)){
            buffer.add(input);
            tuples.add(input);
        }

        CreateInitialCentroids();
    }

    private void CreateInitialCentroids(){
        int index=0;
        Iterator iterator=buffer.iterator();
        Set<String> theAuthors=new HashSet<String>();
        Set<double[]>uniqueVectors=new HashSet<double[]>();
        Set<String>uniqueAuthors=new HashSet<String>();

        Set<List<Double>>uniqueSetOfListsOfFeatures=new HashSet<List<Double>>();
        List<List<Double>>listOfFeatures=new ArrayList<List<Double>>();

        while (iterator.hasNext()){
            Tuple input=(Tuple)iterator.next();

            String author=input.getString(0);
            double[]vector=(double[])input.getValue(1);

            List<Double>aListOfFeatures=new ArrayList<Double>();

            if(vector!=null){
                for(int i=0;i<vector.length;i++){
                    aListOfFeatures.add(vector[i]);
                }
            }

            if(!uniqueSetOfListsOfFeatures.contains(listOfFeatures)){
                listOfFeatures.add(aListOfFeatures);
                uniqueSetOfListsOfFeatures.add(aListOfFeatures);
            }

        }

        if(listOfFeatures.size()==CLUSTERS) {
            //   System.out.println(fuckingListOfFuckingFeatures);
            Iterator iterator1=listOfFeatures.iterator();
            while (iterator1.hasNext()) {
                List<Double> aList = (List<Double>) iterator1.next();

                map.put(index, aList);
                index += 1;
            }
        }

        List<Double>list=new ArrayList<Double>();

        if(!map.isEmpty() && map.entrySet().size()==CLUSTERS && index==CLUSTERS){
            System.out.println(map);

            for(Map.Entry<Integer,List<Double>>entry:map.entrySet()){
                int centroidIndex=(int)entry.getKey();
                List<Double>list1=entry.getValue();

                String[]strings=new String[CLUSTERS];
                serializedVectors= SerializeAndDeserializeJavaObjects.convertDoubleListToString(list1);
                if(serializedVectors!=null) {
                    KClustersDatabase.setSerializedMap(centroidIndex, serializedVectors);
                    System.out.println(centroidIndex+" "+serializedVectors);
                    strings[centroidIndex]=serializedVectors;

                 //   System.out.print(KClustersDatabase.getSerializedClusterMap());
                }


            }
        }
    }
}
