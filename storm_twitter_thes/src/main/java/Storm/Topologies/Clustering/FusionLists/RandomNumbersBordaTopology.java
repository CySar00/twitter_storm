package Storm.Topologies.Clustering.FusionLists;


import Storm.Bolts.ClusteringTechniques.FusionLists.*;

import Storm.Bolts.ClusteringTechniques.Gaussian.EmitRandomNumbersIntoMap;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/31/15.
 */
public class RandomNumbersBordaTopology {

    public static void main(String[]args) throws Exception{

        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_LINES_FROM_FUZZY_CLUSTER_EQUALS_0_TEXT_FILE",new ReadLinesFromTextFile("random_numbers_for_fuzzy_centroid_equals_0.txt"));
        topologyBuilder.setBolt("PROCESS_DATA_FROM_FUZZY_CLUSTER_EQUALS_0_TEXT_FILE",new ProcessDataFromFuzzyCluster0(),8).shuffleGrouping("READ_LINES_FROM_FUZZY_CLUSTER_EQUALS_0_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_0_DATA_INTO_MAP",new EmitDataIntoMaps()).fieldsGrouping("PROCESS_DATA_FROM_FUZZY_CLUSTER_EQUALS_0_TEXT_FILE",new Fields("INDEX"));
        topologyBuilder.setBolt("REMOVE_ANY_DUPLICATE_VALUES_0",new RemoveAnyDuplicateStreams()).shuffleGrouping("EMIT_0_DATA_INTO_MAP");
        topologyBuilder.setBolt("BORDA_COUNT_0",new BordaRank()).shuffleGrouping("REMOVE_ANY_DUPLICATE_VALUES_0");


        topologyBuilder.setSpout("READ_LINES_FROM_FUZZY_CLUSTER_EQUALS_1_TEXT_FILE",new ReadLinesFromTextFile("random_numbers_for_fuzzy_centroid_equals_1.txt"));
        topologyBuilder.setBolt("PROCESS_DATA_FROM_FUZZY_CLUSTER_EQUALS_1_TEXT_FILE",new ProcessDataFromFuzzyCluster1(),8).shuffleGrouping("READ_LINES_FROM_FUZZY_CLUSTER_EQUALS_1_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_1_DATA_INTO_MAP",new EmitDataIntoMaps()).fieldsGrouping("PROCESS_DATA_FROM_FUZZY_CLUSTER_EQUALS_1_TEXT_FILE", new Fields("INDEX"));
        topologyBuilder.setBolt("REMOVE_ANY_DUPLICATE_VALUES_1",new RemoveAnyDuplicateStreams()).shuffleGrouping("EMIT_1_DATA_INTO_MAP");
        topologyBuilder.setBolt("BORDA_COUNT_1",new BordaRank()).shuffleGrouping("REMOVE_ANY_DUPLICATE_VALUES_1");


        topologyBuilder.setSpout("READ_LINES_FROM_FUZZY_CLUSTER_EQUALS_2_TEXT_FILE",new ReadLinesFromTextFile("random_numbers_for_fuzzy_centroid_equals_2.txt"));
        topologyBuilder.setBolt("PROCESS_DATA_FROM_FUZZY_CLUSTER_EQUALS_2_TEXT_FILE",new ProcessDataFromFuzzyCluster2(),8).shuffleGrouping("READ_LINES_FROM_FUZZY_CLUSTER_EQUALS_2_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_2_DATA_INTO_MAP",new EmitDataIntoMaps()).fieldsGrouping("PROCESS_DATA_FROM_FUZZY_CLUSTER_EQUALS_2_TEXT_FILE", new Fields("INDEX"));
        topologyBuilder.setBolt("REMOVE_ANY_DUPLICATE_VALUES_2",new RemoveAnyDuplicateStreams()).shuffleGrouping("EMIT_2_DATA_INTO_MAP");
        topologyBuilder.setBolt("BORDA_COUNT_2",new BordaRank()).shuffleGrouping("REMOVE_ANY_DUPLICATE_VALUES_2");

        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(1 * 60 * 60 * 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
