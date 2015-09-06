package Storm.Topologies;

import Storm.Bolts.CreatingTheDataSet.UsersAndAuthors.WriteToTextFileEverySelectedAuthor;
import Storm.Bolts.FileWriter;
import Storm.Spouts.RandomNumberGeneratorSpout;
import Storm.Spouts.ReadFromFile.ReadLastLineFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/21/15.
 */
public class RandomNumbers {

    public static void main(String[]args ) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("RANDOM_NUMBERS", new RandomNumberGeneratorSpout());
        topologyBuilder.setBolt("WRITE_RANDOM_NUMBERS_TO_FILE", new FileWriter("randomNumbers.txt")).shuffleGrouping("RANDOM_NUMBERS");


        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Test", config, topologyBuilder.createTopology());
            Utils.sleep(10000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }
}
