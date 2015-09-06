package Storm.Topologies.CreatingTheDataSet;


import Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData.FromMongoDBToFurtherProcessing;
import Storm.Bolts.CreatingTheDataSet.ProcessingTweetInfo.FromTweetsFileToFurtherProcessing;
import Storm.Bolts.CreatingTheDataSet.UsersAndAuthors.SelectTheAuthors;
import Storm.Bolts.FileWriter;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/10/15.
 */
public class TheAuthors {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_LINES_FROM_MONGODB",new ReadLinesFromTextFile("tweets.txt"),4);
        topologyBuilder.setBolt("FROM_MONGODB_TO_SELECTING_AUTHORS",new FromTweetsFileToFurtherProcessing(),8).shuffleGrouping("READ_LINES_FROM_MONGODB");

        topologyBuilder.setBolt("SELECT_RANDOM_USERS_FOR_AUTHORS",new SelectTheAuthors()).fieldsGrouping("FROM_MONGODB_TO_SELECTING_AUTHORS", new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_AUTHORS_TO_TEXT_FILE",new FileWriter("authors.txt")).shuffleGrouping("SELECT_RANDOM_USERS_FOR_AUTHORS");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(10*1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }


}
