package storm.topology.Clustering;

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
import storm.spout.LinesSpout;

/**
 * Created by christina on 4/20/15.
 */
public class FuzzyMeansClusteringTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_THE_DATA_FROM_TEXT_FILE",new LinesSpout("data.txt"),4);


        topologyBuilder.setBolt("CREATE_THE_DATA_SET",new CreatingTheDataSet(),16).shuffleGrouping("READ_THE_DATA_FROM_TEXT_FILE");
        topologyBuilder.setBolt("CREATE_THE_INITIAL_FUZZY_CENTROIDS",new CreateTheInitialFuzzyCentroids()).globalGrouping("CREATE_THE_DATA_SET");

        topologyBuilder.setBolt("EMIT_DATA_POINTS_INTO_CLOSEST_FUZZY_CLUSTERS",new EmitDataPointsForFuzzyClustering(),4).shuffleGrouping("CREATE_THE_DATA_SET");
        topologyBuilder.setBolt("FUZZY_CLUSTERS",new FuzzyClusters()).fieldsGrouping("EMIT_DATA_POINTS_INTO_CLOSEST_FUZZY_CLUSTERS",new Fields("INDEX"));

        topologyBuilder.setBolt("CLASSIFY_FUZZY_CLUSTERS_IF_INDEX_IS_1",new FuzzyClusterIndexEquals1()).fieldsGrouping("FUZZY_CLUSTERS",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_CLASSIFIED_DATA_WHERE_CLUSTER_INDEX_IS_1_TO_FILE",new FileWriterBolt("DataFromFuzzyCluster1.txt")).shuffleGrouping("CLASSIFY_FUZZY_CLUSTERS_IF_INDEX_IS_1");

        topologyBuilder.setBolt("CLASSIFY_FUZZY_CLUSTERS_IF_INDEX_IS_0",new FuzzyClusterIndexEquals1()).fieldsGrouping("FUZZY_CLUSTERS",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_CLASSIFIED_DATA_WHERE_CLUSTER_INDEX_IS_0_TO_FILE",new FileWriterBolt("DataFromFuzzyCluster0.txt")).shuffleGrouping("CLASSIFY_FUZZY_CLUSTERS_IF_INDEX_IS_1");

        topologyBuilder.setBolt("CLASSIFY_FUZZY_CLUSTERS_IF_INDEX_IS_2",new FuzzyClusterIndexEquals1()).fieldsGrouping("FUZZY_CLUSTERS",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_CLASSIFIED_DATA_WHERE_CLUSTER_INDEX_IS_2_TO_FILE",new FileWriterBolt("DataFromFuzzyCluster2.txt")).shuffleGrouping("CLASSIFY_FUZZY_CLUSTERS_IF_INDEX_IS_1");




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
