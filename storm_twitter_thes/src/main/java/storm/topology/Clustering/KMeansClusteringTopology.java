package storm.topology.Clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.Clustering.CreatingTheDataSet;

import storm.bolt.Clustering.KClustering.Classify.DataPointsWithClusterIndexEquals0;
import storm.bolt.Clustering.KClustering.Classify.DataPointsWithClusterIndexEquals1;
import storm.bolt.Clustering.KClustering.CreateTheInitialKCentroids;
import storm.bolt.Clustering.KClustering.EmitDataPoints;
import storm.bolt.Clustering.KClustering.KClusters;
import storm.bolt.FileWriterBolt;
import storm.spout.LinesSpout;

/**
 * Created by christina on 3/28/15.
 */
public class KMeansClusteringTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_THE_DATA_FROM_TEXT_FILE",new LinesSpout("data.txt"),1);
        topologyBuilder.setBolt("CREATING_THE_DATA_SET",new CreatingTheDataSet(),16).shuffleGrouping("READ_THE_DATA_FROM_TEXT_FILE");

        topologyBuilder.setBolt("CREATE_THE_INITIAL_K_CENTROIDS",new CreateTheInitialKCentroids()).globalGrouping("CREATING_THE_DATA_SET");

        topologyBuilder.setBolt("EMIT_DATA_POINTS",new EmitDataPoints()).globalGrouping("CREATING_THE_DATA_SET");
        topologyBuilder.setBolt("K_CLUSTERS",new KClusters()).fieldsGrouping("EMIT_DATA_POINTS",new Fields("INDEX"));

        topologyBuilder.setBolt("FIND_CLUSTER_DATA_THAT_HAVE_INDEX_EQUALS_0",new DataPointsWithClusterIndexEquals0()).fieldsGrouping("K_CLUSTERS",new Fields("INDEX","USERNAME"));
        topologyBuilder.setBolt("WRITE_DATA_FROM_CLUSTER_EQUALS_0_TO_FILE",new FileWriterBolt("DataFromKCluster0.txt")).shuffleGrouping("FIND_CLUSTER_DATA_THAT_HAVE_INDEX_EQUALS_0");


        topologyBuilder.setBolt("FIND_CLUSTER_DATA_THAT_HAVE_INDEX_EQUALS_1",new DataPointsWithClusterIndexEquals1()).fieldsGrouping("K_CLUSTERS",new Fields("INDEX","USERNAME"));
        topologyBuilder.setBolt("WRITE_DATA_FROM_CLUSTER_EQUALS_1_TO_FILE",new FileWriterBolt("DataFromKCluster1.txt")).shuffleGrouping("FIND_CLUSTER_DATA_THAT_HAVE_INDEX_EQUALS_1");





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
