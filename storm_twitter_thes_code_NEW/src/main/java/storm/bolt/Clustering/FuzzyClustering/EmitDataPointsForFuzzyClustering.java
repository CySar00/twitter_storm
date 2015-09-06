package storm.bolt.Clustering.FuzzyClustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.bolt.Clustering.Functions.EmitDataIntoClosestCluster;
import storm.bolt.Databases.Cassandra.FuzzyClustersDatabase;

import java.util.*;

/**
 * Created by christina on 4/22/15.
 */
public class EmitDataPointsForFuzzyClustering extends BaseRichBolt {
    public static final double FUZZY=1.7;

    public static final int CLUSTERS=3;
    public static final int TUPLES=10;
    public static final int FEATURES=31;


    OutputCollector collector;
    double[]membershipVector;
    double[][][]ThreeDArray=new double[TUPLES][CLUSTERS][FEATURES];

    List<double[]>listOfMembershipArrays=new ArrayList<double[]>();

    double[]features;
    String author;
    String[]serialized;

    Random random=new Random();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","AUTHOR","VECTOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        author=input.getString(0);
        features=(double[])input.getValue(1);

        serialized= FuzzyClustersDatabase.getSerializedClusterMap();
        Set<String> theFuckingAuthors=new HashSet<String>();

        createVectorsWithRandomNumbers();

        if(author!=null && features!=null && ThreeDArray!=null) {

            //  System.out.println(author);
            for (double[] aListEntry : listOfMembershipArrays) {
                if (aListEntry != null) {
                   // System.out.println(aListEntry);
                    EmitDataIntoClosestCluster.emitDataPointsIntoClosestFuzzyCluster(collector, serialized, author, features, aListEntry, FUZZY);

                }
            }
        }
    }

    private void createVectorsWithRandomNumbers(){

        for(int i=0;i<TUPLES;i++){
            for(int j=0;j<CLUSTERS;j++){
                for(int k=0;k<FEATURES;k++){
                    ThreeDArray[i][j][k]=random.nextDouble()+0.05;
                }
            }
        }

        for(int i=0;i<TUPLES;i++){
            for(int j=0;j<CLUSTERS;j++){
                listOfMembershipArrays.add(ThreeDArray[i][j]);
            }
        }
    }
}
