package Storm.Topologies.CreatingTheDataSet;

import Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData.SearchingForAuthorsTweetData;
import Storm.Bolts.CreatingTheDataSet.ProcessingTweetInfo.FromTweetsFileToFurtherProcessing;
import Storm.Bolts.CreatingTheDataSet.UsersAndAuthors.WriteAuthorsFromTextFileToMongoDB;
import Storm.Bolts.Databases.MongoDB.WriteTheAuthorsTweetFeedsToMongoDB;
import Storm.Bolts.Databases.MongoDB.WriteTheSelectedAuthorsToMongoDB;
import Storm.Spouts.ReadFromFile.ReadLastLineFromTextFile;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/23/15.
 */
public class TheAuthorsAndEtcTweetData {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_LAST_LINE_FROM_AUTHORS_TEXT_FILE",new ReadLastLineFromTextFile("authors.txt"));
        topologyBuilder.setBolt("PROCESS_READ_AUTHORS_FROM_FILE_TO_MONGODB",new WriteAuthorsFromTextFileToMongoDB()).shuffleGrouping("READ_LAST_LINE_FROM_AUTHORS_TEXT_FILE");

        topologyBuilder.setSpout("READ_TWEETS_SAVED_TO_MONGODB_TEXT_FILE",new ReadLinesFromTextFile("tweets.txt"));
        topologyBuilder.setBolt("PROCESS_THE_TWEETS",new FromTweetsFileToFurtherProcessing(),16).shuffleGrouping("READ_TWEETS_SAVED_TO_MONGODB_TEXT_FILE");

        topologyBuilder.setBolt("SEARCH",new SearchingForAuthorsTweetData(),16).fieldsGrouping("PROCESS_THE_TWEETS",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_AUTHORS_AND_THEIR_TWEET_DATA_TO_MONGODB",new WriteTheAuthorsTweetFeedsToMongoDB(),16).shuffleGrouping("SEARCH");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(10*10* 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }
}
