package storm.topology.GaussianRank;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.FileWriterBolt;
import storm.bolt.GaussianRankAndMixtureModel.DataMaps.CreateDataSetFromAKCluster;
import storm.bolt.GaussianRankAndMixtureModel.DataMaps.EmitAllDataPointsFromAKClusterIntoMap;
import storm.spout.LinesSpout;

/**
 * Created by christina on 4/3/15.
 */
public class CreateMapFromKClustersForGaussianRankTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();


//        topologyBuilder.setSpout("READ_DATA_WITH_K_CLUSTER_EQUALS_0",new LinesSpout("DataFromKCluster0.txt"));
  //      topologyBuilder.setBolt("CREATE_K_CLUSTER_EQUALS_0_DATA_SET",new CreateDataSetFromAKCluster()).shuffleGrouping("READ_DATA_WITH_K_CLUSTER_INDEX_EQUALS_0");

    //    topologyBuilder.setBolt("EMIT_DATA_FROM_K_CLUSTER_EQUALS_0_DATA_SET_INTO_MAP",new EmitAllDataPointsFromAKClusterIntoMap()).fieldsGrouping("CREATE_K_CLUSTER_EQUALS_0_DATA_SET", new Fields("FUCKING_AUTHOR"));
      //  topologyBuilder.setBolt("WRITE_MAP_WITH_DATA_FROM_K_CLUSTER_EQUALS_0_TO_TEXT_FILE",new FileWriterBolt("WriteKClusterMapWithIndex0ToFile.txt")).shuffleGrouping("EMIT_DATA_FROM_K_CLUSTER_EQUALS_0_DATA_SET_INTO_MAP");

        topologyBuilder.setSpout("READ_DATA_WITH_K_CLUSTER_INDEX_EQUALS_1",new LinesSpout("DataFromKCluster1.txt"));
        topologyBuilder.setBolt("CREATE_K_CLUSTER_EQUALS_1_DATA_SET",new CreateDataSetFromAKCluster()).shuffleGrouping("READ_DATA_WITH_K_CLUSTER_INDEX_EQUALS_1");

        topologyBuilder.setBolt("EMIT_DATA_FROM_K_CLUSTER_EQUALS_1_DATA_SET_INTO_MAP",new EmitAllDataPointsFromAKClusterIntoMap()).fieldsGrouping("CREATE_K_CLUSTER_EQUALS_1_DATA_SET", new Fields("FUCKING_AUTHOR"));
        topologyBuilder.setBolt("WRITE_MAP_WITH_DATA_FROM_K_CLUSTER_EQUALS_1_TO_TEXT_FILE",new FileWriterBolt("WriteKClusterMapWithIndex1ToFile.txt")).shuffleGrouping("EMIT_DATA_FROM_K_CLUSTER_EQUALS_1_DATA_SET_INTO_MAP");





        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(900000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
