package Storm.Topologies.Clustering.Gaussian.GMM.GMM_Centroid_1;


import Storm.Bolts.ClusteringTechniques.Gaussian.EmitAuthorAndFeaturesIntoMap;
import Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Cluster_Centroid_Equals_1.ProcessAuthorAndFeaturesFromCentroidEquals1TextFile;
import Storm.Bolts.FileWriter;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/18/15.
 */
public class TempMapForKClusterCentroidEquals1 {

    public static void main(String[]args) throws Exception{

        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_AND_INDEX_CENTROID_IS_ONE_TEXT_FILE", new ReadLinesFromTextFile("authors_with_cluster_index_equals_1.txt"));
        topologyBuilder.setBolt("PROCESS_AUTHOR_AND_FEATURES_FROM_CENTROID_IS_1_TEXT_FILE",new ProcessAuthorAndFeaturesFromCentroidEquals1TextFile()).shuffleGrouping("READ_AND_INDEX_CENTROID_IS_ONE_TEXT_FILE");

        topologyBuilder.setBolt("EMIT_AUTHOR_AND_FEATURES_INTO_MAP",new EmitAuthorAndFeaturesIntoMap()).shuffleGrouping("PROCESS_AUTHOR_AND_FEATURES_FROM_CENTROID_IS_1_TEXT_FILE");
        topologyBuilder.setBolt("WRITE_MAP_TO_TEXT_FILE",new FileWriter("temp.txt")).shuffleGrouping("EMIT_AUTHOR_AND_FEATURES_INTO_MAP");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(5 * 60  * 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }
}

