package Storm.Topologies.CreatingTheDataSet;

import Storm.Bolts.CreatingTheDataSet.UsersAndAuthors.WriteToTextFileEverySelectedAuthor;
import Storm.Bolts.FileWriter;
import Storm.Spouts.ReadFromFile.ReadLastLineFromTextFile;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/16/15.
 */
public class AnAuthor {

    public static void main(String[]args ) throws Exception{
    TopologyBuilder topologyBuilder=new TopologyBuilder();

    topologyBuilder.setSpout("READ_THE_AUTHORS_FROM_TEXT_FILE",new ReadLastLineFromTextFile("authors.txt"));
    topologyBuilder.setBolt("FROM_THE_AUTHORS_TEXT_FILE_TO_FURTHER_PROCESSING", new WriteToTextFileEverySelectedAuthor()).shuffleGrouping("READ_THE_AUTHORS_FROM_TEXT_FILE");
    topologyBuilder.setBolt("WRITE_AUTHOR_TO_TEXT_FILE",new FileWriter("anAuthor.txt")).shuffleGrouping("FROM_THE_AUTHORS_TEXT_FILE_TO_FURTHER_PROCESSING");



    Config config=new Config();
    if(args!=null && args.length>0){
        config.setNumWorkers(10);
        config.setNumAckers(5);
        config.setMaxSpoutPending(100);
        StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
    }else{
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
        Utils.sleep( 60*1000);
        localCluster.killTopology("Test");
        localCluster.shutdown();
    }
}
}
