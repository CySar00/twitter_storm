package Storm.Topologies.Clustering.Gaussian.GMM.GMM_Centroid_1;


import Storm.Bolts.ClusteringTechniques.Gaussian.GMM.InitialValues.EmitMap;

import Storm.Spouts.ReadFromFile.ReadLastLineFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/20/15.
 */
public class GMM_Try_1 {

    public static void main(String[]args) throws Exception{

        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MAP_FROM_TEXT_FILE",new ReadLastLineFromTextFile("temp.txt"));
        topologyBuilder.setBolt("CALCULATE",new EmitMap()).shuffleGrouping("READ_MAP_FROM_TEXT_FILE");



        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(30 * 60 * 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }
}
