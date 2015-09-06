package Storm.Topologies.Clustering.ClassicClustering.KMeans;

import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Classify.AuthorsThatHaveClusterIndexEquals0;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Classify.AuthorsThatHaveClusterIndexEquals1;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Clustrering.EmitAuthorAndDoublesVectorForKClustering;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Clustrering.KClustering;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.CreatingTheInitialCentroids.CreateTheInitialKCentroids;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.CreatingTheInitialCentroids.ProcessingTextFileToCreateKInitialClusters;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.ProcessingDataSetForClassicClustering;
import Storm.Bolts.FileWriter;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import Storm.Spouts.ReadFromFile.ReadNRandomLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/11/15.
 */
public class KMeansClusteringTopology {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_TWO_LINES_FROM_METRICS_AND_FEATURES_TEXT_FILE",new ReadNRandomLinesFromTextFile("metrics_and_features.txt",2));
        topologyBuilder.setBolt("PROCESS_THE_TWO_LINES_TO_CREATE_INITIAL_CENTROIDS",new ProcessingTextFileToCreateKInitialClusters()).shuffleGrouping("READ_TWO_LINES_FROM_METRICS_AND_FEATURES_TEXT_FILE");
        topologyBuilder.setBolt("CREATE_THE_INITIAL_CENTROIDS",new CreateTheInitialKCentroids()).shuffleGrouping("PROCESS_THE_TWO_LINES_TO_CREATE_INITIAL_CENTROIDS");

        topologyBuilder.setSpout("READ_AUTHOR_METRICS_AND_FEATURES_FROM_TEXT_FILE",new ReadLinesFromTextFile("metrics_and_features.txt"));
        topologyBuilder.setBolt("PROCESS_AUTHOR_METRICS_AND_FEATURES_TO_INITIATE_CLUSTERING",new ProcessingDataSetForClassicClustering()).shuffleGrouping("READ_AUTHOR_METRICS_AND_FEATURES_FROM_TEXT_FILE");

        topologyBuilder.setBolt("EMIT_AUTHOR_AND_VECTOR_FOR_K_CLUSTERING",new EmitAuthorAndDoublesVectorForKClustering()).fieldsGrouping("PROCESS_AUTHOR_METRICS_AND_FEATURES_TO_INITIATE_CLUSTERING",new Fields("USERNAME"));

        topologyBuilder.setBolt("K_CLUSTERING",new KClustering()).fieldsGrouping("EMIT_AUTHOR_AND_VECTOR_FOR_K_CLUSTERING",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_CLUSTERING_RESULTS_TO_TEXT_FILE",new FileWriter("Kclusters.txt")).shuffleGrouping("K_CLUSTERING");

        topologyBuilder.setBolt("COLLECT_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_0",new AuthorsThatHaveClusterIndexEquals0()).fieldsGrouping("K_CLUSTERING",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_0_TO_TEXT_FILE",new FileWriter("authors_with_cluster_index_equals_0.txt")).shuffleGrouping("COLLECT_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_0");

        topologyBuilder.setBolt("COLLECT_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_1",new AuthorsThatHaveClusterIndexEquals1()).fieldsGrouping("K_CLUSTERING",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_1_TO_TEXT_FILE",new FileWriter("authors_with_cluster_index_equals_1.txt")).shuffleGrouping("COLLECT_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_1");


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
