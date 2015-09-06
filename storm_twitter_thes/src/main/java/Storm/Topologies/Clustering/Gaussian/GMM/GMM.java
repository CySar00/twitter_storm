package Storm.Topologies.Clustering.Gaussian.GMM;

import Storm.Bolts.ClusteringTechniques.Gaussian.GMM.InitialValues.EmitMap;
import Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank.CalculatingTheCDFs;
import Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank.TheRank;
import Storm.Bolts.ClusteringTechniques.Gaussian.ProcessIndexedLinesOfRandomNumbers;
import Storm.Bolts.ClusteringTechniques.Gaussian.ReadRandomNumbersFromFile;
import Storm.Bolts.FileWriter;
import Storm.Spouts.ReadFromFile.ReadAndIndexLinesRead;
import Storm.Spouts.ReadFromFile.ReadLastLineFromTextFile;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/21/15.
 */
public class GMM {
    public static void main(String[]args) throws Exception{

        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MAP_FROM_TEXT_FILE", new ReadLastLineFromTextFile("random_numbers_map.txt"));
        topologyBuilder.setBolt("CALCULATE",new EmitMap()).shuffleGrouping("READ_MAP_FROM_TEXT_FILE");

        topologyBuilder.setSpout("READ_RANDOM_NUMBERS_FROM_FILE", new ReadAndIndexLinesRead("randomNumbers.txt"));
        topologyBuilder.setBolt("PROCESS_INDEXED_LINES",new ProcessIndexedLinesOfRandomNumbers()).shuffleGrouping("READ_RANDOM_NUMBERS_FROM_FILE");

        topologyBuilder.setBolt("CALCULATE_THE_CDFS",new CalculatingTheCDFs()).fieldsGrouping("PROCESS_INDEXED_LINES",new Fields("INDEX"));
        topologyBuilder.setBolt("WRITE_CDFS_TO_TEXT_FILE",new FileWriter("cdfs_of_the_random_numbers.txt")).shuffleGrouping("CALCULATE_THE_CDFS");

        topologyBuilder.setBolt("RANK",new TheRank()).fieldsGrouping("CALCULATE_THE_CDFS",new Fields("AUTHOR"));
        topologyBuilder.setBolt("WRITE_RANKS_TO_FILE",new FileWriter("ranked_numbers_of_ranked_cdfs.txt")).shuffleGrouping("RANK");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(5*1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
