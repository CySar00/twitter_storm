package storm.bolt.Clustering.KClustering.KMeans;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;

/**
 * Created by christina on 5/29/15.
 */
public class KMeans extends BaseBasicBolt {
    public static final int CLUSTERS=2;

    public static final int USERS=89;
    Buffer buffer=new CircularFifoBuffer(USERS);

    private Map<String,double[]>data=new HashMap<String, double[]>();
    private Map<Integer,double[]>centroids=new HashMap<Integer, double[]>();

    Random random=new Random();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Set<Tuple> setOfTuples=new HashSet<Tuple>();
        Map<String,double[]>tempMap1=new HashMap<String, double[]>();

        if(!setOfTuples.contains(setOfTuples)) {
            buffer.add(input);
            setOfTuples.add(input);


        }

        createTheDataSet();
        if(!data.isEmpty() && data.entrySet().size()==USERS) {




            if(!centroids.isEmpty() && centroids.entrySet().size()==CLUSTERS){
                System.out.println(centroids);
                System.out.println("f***");
                }
            }



    }

    private void createTheDataSet(){
        int index=0;

        Set<String>theAuthors=new HashSet<String>();
        Set<double[]>theVectors=new HashSet<double[]>();
        List<double[]>vectors=new ArrayList<double[]>();

        Iterator iterator=buffer.iterator();
        while (iterator.hasNext()){
            Tuple aTuple=(Tuple)iterator.next();

            if(aTuple!=null) {

                String anAuthor = aTuple.getString(0);
                double[] aVectorOfFeatures = (double[]) aTuple.getValue(1);

                if (anAuthor != null && aVectorOfFeatures != null) {

                    if (!theAuthors.contains(anAuthor)) {
                        if (!data.containsKey(anAuthor)) {
                            data.put(anAuthor, aVectorOfFeatures);
                            theAuthors.add(anAuthor);

                            if(!theVectors.contains(aVectorOfFeatures)){
                                vectors.add(aVectorOfFeatures);
                                theVectors.add(aVectorOfFeatures);
                            }
                        }
                    }
                }
            }else{
                break;
            }
        }

        if(vectors!=null || !vectors.isEmpty()){
            for(double[]aVector:vectors){

                if(aVector==vectors.get(random.nextInt(vectors.size()))){
                    centroids.put(index,aVector);
                    index+=1;

                    if(index==CLUSTERS)
                        break;

                }
            }


        }
    }






    private double computeDistance(double[]centroidVector,double[]dataVector){
        double sqSum=0;
        double manhattanSum=0;

        for(int i=0;i<centroidVector.length;i++){
            sqSum+=(centroidVector[i]-dataVector[i])*(centroidVector[i]-dataVector[i]);
            manhattanSum+=Math.abs(centroidVector[i]-dataVector[i]);
        }
        return Math.sqrt(sqSum);
    }

    private void KMeans(Map<Integer,double[]>centroids,double[]dataVector){
        double min=Double.MAX_VALUE;
        double diff=0;
        int  index=0;

        for(Map.Entry<Integer,double[]>centroidEntry:centroids.entrySet()){
            int centroidIndex=(int)centroidEntry.getKey();
            double[]centroidVector=centroidEntry.getValue();

            diff=computeDistance(centroidVector,dataVector);
            if(diff<min){
                min=diff;
                index=centroidIndex;
            }
        }



    }










}
