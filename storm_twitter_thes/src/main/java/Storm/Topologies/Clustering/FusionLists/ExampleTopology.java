package Storm.Topologies.Clustering.FusionLists;

import Storm.Bolts.ClusteringTechniques.FusionLists.Example.Example;
import Storm.Bolts.ClusteringTechniques.FusionLists.Example.Example1;
import Storm.Spouts.MapWithDoublesVectorSpout;
import Storm.Spouts.ScoresMapSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/31/15.
 */
public class ExampleTopology {
    public static void main(String[]args) throws Exception{

        TopologyBuilder topologyBuilder=new TopologyBuilder();
      //  topologyBuilder.setSpout("SCORES_SPOUT",new ScoresMapSpout());
        //topologyBuilder.setBolt("BORDA",new Example()).shuffleGrouping("SCORES_SPOUT");


        topologyBuilder.setSpout("CREATE_MAP_SPOUT",new MapWithDoublesVectorSpout());
        topologyBuilder.setBolt("BORDA",new Example1()).shuffleGrouping("CREATE_MAP_SPOUT");



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
