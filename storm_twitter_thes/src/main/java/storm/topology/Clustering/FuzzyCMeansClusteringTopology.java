package storm.topology.Clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.bolt.Clustering.CreatingTheDataSet;

import storm.bolt.Clustering.FuzzyClustering.CreateTheInitialFuzzyCentroids;
import storm.bolt.Clustering.FuzzyClustering.MembershipVector;
import storm.spout.LinesSpout;
import storm.spout.ThreeDRandomNumberSpout;

/**
 * Created by christina on 3/28/15.
 */
public class FuzzyCMeansClusteringTopology {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_THE_DATA_FROM_TEXT_FILE",new LinesSpout("data.txt"));
        topologyBuilder.setBolt("CREATE_THE_DATA_SET_FOR_FUZZY_MEANS",new CreatingTheDataSet()).shuffleGrouping("READ_THE_DATA_FROM_TEXT_FILE");

        topologyBuilder.setSpout("CREATE_MEMBERSHIP_ARRAY", new ThreeDRandomNumberSpout());
        topologyBuilder.setBolt("MEMBERSHIP_VECTOR",new MembershipVector()).shuffleGrouping("CREATE_MEMBERSHIP_ARRAY");

        topologyBuilder.setBolt("CREATE_THE_INITIAL_FUZZY_CENTROIDS",new CreateTheInitialFuzzyCentroids()).globalGrouping("CREATE_THE_DATA_SET_FOR_FUZZY_MEANS");






        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(100000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}


