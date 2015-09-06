package Storm.Topologies;

import Storm.Bolts.ClusteringTechniques.Gaussian.EmitRandomNumbersIntoMap;
import Storm.Bolts.ClusteringTechniques.Gaussian.ReadRandomNumbersFromFile;
import Storm.Bolts.FileWriter;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/21/15.
 */
public class ReadRandomNumbers {

    public static void main(String[]args ) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("READ_RANDOM_NUMBERS_FROM_FILE", new ReadLinesFromTextFile("randomNumbers.txt"));
        topologyBuilder.setBolt("PROCESS_RANDOM_NUMBERS",new ReadRandomNumbersFromFile()).shuffleGrouping("READ_RANDOM_NUMBERS_FROM_FILE");
        topologyBuilder.setBolt("EMIT_RANDOM_NUMBERS_INTO_MAP",new EmitRandomNumbersIntoMap()).shuffleGrouping("PROCESS_RANDOM_NUMBERS");
        topologyBuilder.setBolt("WRITE_MAP_TO_FILE",new FileWriter("random_numbers_map.txt")).shuffleGrouping("EMIT_RANDOM_NUMBERS_INTO_MAP");



        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Test", config, topologyBuilder.createTopology());
            Utils.sleep(60 *10* 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }
}
