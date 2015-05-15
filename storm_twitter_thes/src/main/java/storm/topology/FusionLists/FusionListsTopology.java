package storm.topology.FusionLists;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.Clustering.CreatingTheDataSet;
import storm.bolt.Clustering.FuzzyClustering.CreateTheInitialFuzzyCentroids;
import storm.bolt.Clustering.FuzzyClustering.EmitDataPointsForFuzzyClustering;
import storm.bolt.Clustering.FuzzyClustering.FuzzyClusters;
import storm.bolt.Clustering.FuzzyClustering.classify.FuzzyClusterIndexEquals1;
import storm.bolt.FileWriterBolt;
import storm.bolt.FusionLists.CreateFusionListsForClusterIndexEquals0;
import storm.bolt.FusionLists.CreateFusionListsForClusterIndexEquals1;
import storm.bolt.FusionLists.CreateFusionListsForClusterIndexEquals2;
import storm.bolt.FusionLists.ReadData.EmitAuthorAndListOfFeaturesIntoMap;
import storm.bolt.FusionLists.ReadData.ProcessDataFromTextFile;
import storm.spout.LinesSpout;
import storm.spout.ReadLastLineSpout;

/**
 * Created by christina on 3/28/15.
 */
public class FusionListsTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_DATA_FROM_CLUSTER_INDEX_IS_0_TEXT_FILE", new LinesSpout("DataFromFuzzyCluster0.txt"), 4);
        topologyBuilder.setBolt("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_0",new ProcessDataFromTextFile(),16).shuffleGrouping("READ_DATA_FROM_CLUSTER_INDEX_IS_0_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_DATA_INTO_MAP_WHERE_CLUSTER_INDEX_IS_0",new EmitAuthorAndListOfFeaturesIntoMap()).globalGrouping("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_0");
        topologyBuilder.setBolt("CREATE_FUSION_LISTS_FOR_CLUSTER_INDEX_EQUALS_0",new CreateFusionListsForClusterIndexEquals1()).globalGrouping("EMIT_DATA_INTO_MAP_WHERE_CLUSTER_INDEX_IS_0");
        topologyBuilder.setBolt("WRITE_FUSION_LISTS_0_TO_TEXT_FILE",new FileWriterBolt("FusionLists0.txt")).shuffleGrouping("CREATE_FUSION_LISTS_FOR_CLUSTER_INDEX_EQUALS_0");

        topologyBuilder.setSpout("READ_DATA_FROM_CLUSTER_INDEX_IS_1_TEXT_FILE", new LinesSpout("DataFromFuzzyCluster1.txt"), 4);
        topologyBuilder.setBolt("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_1",new ProcessDataFromTextFile(),16).shuffleGrouping("READ_DATA_FROM_CLUSTER_INDEX_IS_1_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_DATA_INTO_MAP_WHERE_CLUSTER_INDEX_IS_1",new EmitAuthorAndListOfFeaturesIntoMap()).globalGrouping("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_1");
        topologyBuilder.setBolt("CREATE_FUSION_LISTS_FOR_CLUSTER_INDEX_EQUALS_1",new CreateFusionListsForClusterIndexEquals1()).globalGrouping("EMIT_DATA_INTO_MAP_WHERE_CLUSTER_INDEX_IS_1");
        topologyBuilder.setBolt("WRITE_FUSION_LISTS_1_TO_TEXT_FILE",new FileWriterBolt("FusionLists1.txt")).shuffleGrouping("CREATE_FUSION_LISTS_FOR_CLUSTER_INDEX_EQUALS_1");

        topologyBuilder.setSpout("READ_DATA_FROM_CLUSTER_INDEX_IS_2_TEXT_FILE", new LinesSpout("DataFromFuzzyCluster2.txt"), 4);
        topologyBuilder.setBolt("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_2",new ProcessDataFromTextFile(),16).shuffleGrouping("READ_DATA_FROM_CLUSTER_INDEX_IS_2_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_DATA_INTO_MAP_WHERE_CLUSTER_INDEX_IS_2",new EmitAuthorAndListOfFeaturesIntoMap()).globalGrouping("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_2");
        topologyBuilder.setBolt("CREATE_FUSION_LISTS_FOR_CLUSTER_INDEX_EQUALS_2",new CreateFusionListsForClusterIndexEquals1()).globalGrouping("EMIT_DATA_INTO_MAP_WHERE_CLUSTER_INDEX_IS_2");
        topologyBuilder.setBolt("WRITE_FUSION_LISTS_2_TO_TEXT_FILE",new FileWriterBolt("FusionLists2.txt")).shuffleGrouping("CREATE_FUSION_LISTS_FOR_CLUSTER_INDEX_EQUALS_2");



        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(9000000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
