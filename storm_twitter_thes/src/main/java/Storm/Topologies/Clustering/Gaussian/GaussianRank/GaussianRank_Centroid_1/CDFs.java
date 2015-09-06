package Storm.Topologies.Clustering.Gaussian.GaussianRank.GaussianRank_Centroid_1;

import Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank.GaussianRank_For_Centroid_Index_Equals_1.ReadValuesFromEMFile;
import Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Cluster_Centroid_Equals_1.ProcessAuthorAndFeaturesFromCentroidEquals1TextFile;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/20/15.
 */
public class CDFs {

    public static void main(String[]args) throws Exception{

        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("READ_AND_INDEX_CENTROID_IS_ONE_TEXT_FILE", new ReadLinesFromTextFile("authors_with_cluster_index_equals_1.txt"));
        topologyBuilder.setBolt("PROCESS_AUTHOR_AND_FEATURES_FROM_CENTROID_IS_1_TEXT_FILE",new ProcessAuthorAndFeaturesFromCentroidEquals1TextFile()).shuffleGrouping("READ_AND_INDEX_CENTROID_IS_ONE_TEXT_FILE");

        topologyBuilder.setSpout("READ_EM_VALUES_AND_AUTHOR_FROM_TEXT_FILE",new ReadLinesFromTextFile("em_1.txt"));
        topologyBuilder.setBolt("PROCESS_AUTHORS_AND_EM_VALUES_FOR_FURTHER_PROCESSING",new ReadValuesFromEMFile()).shuffleGrouping("READ_EM_VALUES_AND_AUTHOR_FROM_TEXT_FILE");



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
