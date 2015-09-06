package Storm.Topologies.Clustering.ClassicClustering.CMeans;

import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Classify.AuthorsThatHaveClusteredIndexEquals0;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Classify.AuthorsThatHaveClusteredIndexEquals1;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Classify.AuthorsThatHaveClusteredIndexEquals2;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Clustering.EmitAuthorAndDoublesVectorForFuzzyClustering;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.Clustering.FuzzyClustering;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.CMeans.CreatingTheInitialCentroids.CreateTheInitialFuzzyClusters;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Classify.AuthorsThatHaveClusterIndexEquals0;
import Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.Classify.AuthorsThatHaveClusterIndexEquals1;
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
public class FuzzyCMeansClusteringTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_THREE_LINES_FROM_METRICS_AND_FEATURES_TEXT_FILE",new ReadNRandomLinesFromTextFile("metrics_and_features.txt",3));
        topologyBuilder.setBolt("PROCESS_THE_THREE_LINES_TO_CREATE_INITIAL_FUZZY_CENTROIDS",new ProcessingTextFileToCreateKInitialClusters()).shuffleGrouping("READ_THREE_LINES_FROM_METRICS_AND_FEATURES_TEXT_FILE");
        topologyBuilder.setBolt("CREATE_THE_INITIAL_FUZZY_CENTROIDS",new CreateTheInitialFuzzyClusters()).shuffleGrouping("PROCESS_THE_THREE_LINES_TO_CREATE_INITIAL_FUZZY_CENTROIDS");

        topologyBuilder.setSpout("READ_AUTHOR_METRICS_AND_FEATURES_FROM_TEXT_FILE",new ReadLinesFromTextFile("metrics_and_features.txt"));
        topologyBuilder.setBolt("PROCESS_AUTHOR_METRICS_AND_FEATURES_TO_INITIATE_CLUSTERING",new ProcessingDataSetForClassicClustering()).shuffleGrouping("READ_AUTHOR_METRICS_AND_FEATURES_FROM_TEXT_FILE");

        topologyBuilder.setBolt("EMIT_AUTHOR_AND_VECTOR_FOR_FUZZY_CLUSTERING",new EmitAuthorAndDoublesVectorForFuzzyClustering()).fieldsGrouping("PROCESS_AUTHOR_METRICS_AND_FEATURES_TO_INITIATE_CLUSTERING",new Fields("USERNAME"));
        topologyBuilder.setBolt("FUZZY_CLUSTERING",new FuzzyClustering()).fieldsGrouping("EMIT_AUTHOR_AND_VECTOR_FOR_FUZZY_CLUSTERING",new Fields("INDEX"));

        topologyBuilder.setBolt("COLLECT_ALL_AUTHORS_THAT_HAVE_FUZZY_CLUSTER_INDEX_EQUALS_0",new AuthorsThatHaveClusteredIndexEquals0()).fieldsGrouping("FUZZY_CLUSTERING", new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_0_TO_TEXT_FILE",new FileWriter("authors_with_fuzzy_cluster_index_equals_0.txt")).shuffleGrouping("COLLECT_ALL_AUTHORS_THAT_HAVE_FUZZY_CLUSTER_INDEX_EQUALS_0");

        topologyBuilder.setBolt("COLLECT_ALL_AUTHORS_THAT_HAVE_FUZZY_CLUSTER_INDEX_EQUALS_1",new AuthorsThatHaveClusteredIndexEquals1()).fieldsGrouping("FUZZY_CLUSTERING", new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_1_TO_TEXT_FILE",new FileWriter("authors_with_fuzzy_cluster_index_equals_1.txt")).shuffleGrouping("COLLECT_ALL_AUTHORS_THAT_HAVE_FUZZY_CLUSTER_INDEX_EQUALS_1");

        topologyBuilder.setBolt("COLLECT_ALL_AUTHORS_THAT_HAVE_FUZZY_CLUSTER_INDEX_EQUALS_2",new AuthorsThatHaveClusteredIndexEquals2()).fieldsGrouping("FUZZY_CLUSTERING", new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_ALL_AUTHORS_THAT_HAVE_CLUSTER_INDEX_EQUALS_2_TO_TEXT_FILE",new FileWriter("authors_with_fuzzy_cluster_index_equals_2.txt")).shuffleGrouping("COLLECT_ALL_AUTHORS_THAT_HAVE_FUZZY_CLUSTER_INDEX_EQUALS_2");


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
