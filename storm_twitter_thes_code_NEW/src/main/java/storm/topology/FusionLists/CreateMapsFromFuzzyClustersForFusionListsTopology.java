package storm.topology.FusionLists;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.Clustering.FuzzyClustering.EmitDataPointsForFuzzyClustering;
import storm.bolt.FileWriterBolt;
import storm.bolt.FusionLists.ReadData.EmitAuthorAndListOfFeaturesIntoMap;
import storm.bolt.FusionLists.ReadData.ProcessDataFromTextFile;
import storm.spout.LinesSpout;

/**
 * Created by christina on 5/5/15.
 */
public class CreateMapsFromFuzzyClustersForFusionListsTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_DATA_FROM_CLUSTER_INDEX_IS_1_TEXT_FILE",new LinesSpout("DataFromFuzzyCluster1.txt"),4);
        topologyBuilder.setBolt("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_1",new ProcessDataFromTextFile(),16).shuffleGrouping("READ_DATA_FROM_CLUSTER_INDEX_IS_1_TEXT_FILE");
        topologyBuilder.setBolt("EMIT_DATA_INTO_MAP",new EmitAuthorAndListOfFeaturesIntoMap()).globalGrouping("PROCESS_DATA_FROM_TEXT_FILE_WHERE_CLUSTER_INDEX_IS_1");
        topologyBuilder.setBolt("WRITE_MAP_TO_A_TEXT_FILE",new FileWriterBolt("FusionDataWhereClusterIndexIs1.txt")).shuffleGrouping("EMIT_DATA_INTO_MAP");


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
