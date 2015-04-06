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

    Map<Integer,double[]>map=new HashMap<Integer, double[]>();
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
        Set<String> theFuckingAuthors=new HashSet<String>();
        while (iterator.hasNext()){
            Tuple input=(Tuple)iterator.next();

            String author=input.getString(0);
            double[]vector=(double[])input.getValue(1);

            if(!theFuckingAuthors.contains(author)) {

                if (!map.containsKey(index) && !(map.containsValue(vector))) {
                    map.put(index, vector);
                }

                if (index == CLUSTERS) {
                    break;
                }
                index += 1;
                theFuckingAuthors.add(author);
            }
        }

        List<Double>list=new ArrayList<Double>();

        if(!map.isEmpty() && map.entrySet().size()==CLUSTERS && index==CLUSTERS){
            System.out.println(map);

            for(Map.Entry<Integer,double[]>entry:map.entrySet()){
                int centroidIndex=(int)entry.getKey();
                double[]centroidVector=entry.getValue();

                String[]strings=new String[CLUSTERS];
                serializedVectors= SerializeAndDeserializeJavaObjects.convertDoubleVectorToString(centroidVector);
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
