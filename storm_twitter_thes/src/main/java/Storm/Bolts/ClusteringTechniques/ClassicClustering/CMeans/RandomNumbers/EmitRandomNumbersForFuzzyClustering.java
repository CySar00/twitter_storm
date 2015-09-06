package Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.RandomNumbers;

import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Functions.EmitAuthorAndDoublesVectorIntoClosestFuzzyCluster;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.RandomNumbers.Functions.findClosestFuzzyClusters;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.primitives.Doubles;

import java.util.*;

/**
 * Created by christina on 7/27/15.
 */
public class EmitRandomNumbersForFuzzyClustering extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,List<Double>>map1;
    Map<String,double[]>map2;
    Random random;

    public static final double FUZZY=1.7;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("INDEX","INDEX_1","VECTOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        map1=new HashMap<String, List<Double>>();
        map2=new HashMap<String, double[]>();

        random=new Random();
    }

    @Override
    public void execute(Tuple input) {
        String index1=input.getString(0);
        List<Double>list=(List<Double>)input.getValue(1);

        double[]vector= Doubles.toArray(list);

        List<Double>listOfMap1=map1.get(index1);
        if(listOfMap1==null){
            listOfMap1=new ArrayList<Double>();
        }
        listOfMap1=list;


        double[]vectorOfMap2=map2.get(index1);
        if(vectorOfMap2==null){
            vectorOfMap2=new double[vector.length];
        }
        vectorOfMap2=vector;

        String[] serialized=Storm.Databases.CassandraDB.Clustering.CCentroidsDatabase.getSerializedClusterMap();
        double[]membership=new double[vector.length];
        for(int i=0;i<membership.length;i++){
            membership[i]=random.nextDouble();
        }
    //    System.out.println(Doubles.asList(membership));
        findClosestFuzzyClusters.emit(collector,serialized,index1,vector,membership,FUZZY);
    }
}
