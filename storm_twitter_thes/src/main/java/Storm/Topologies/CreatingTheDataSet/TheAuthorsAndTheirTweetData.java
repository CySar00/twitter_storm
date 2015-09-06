package Storm.Topologies.CreatingTheDataSet;



import Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData.SearchForTheAuthorsTweetData;
import Storm.Bolts.CreatingTheDataSet.ProcessingTweetInfo.FromTweetsFileToFurtherProcessing;
import Storm.Bolts.CreatingTheDataSet.UsersAndAuthors.ProcessAuthors;
import Storm.Bolts.Databases.MongoDB.WriteTheAuthorsTweetFeedsToMongoDB;
import Storm.Bolts.FileWriter;
import Storm.Bolts.SingleJoinBolt;
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
public class TheAuthorsAndTheirTweetData {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_AUTHORS_MONGO",new ReadLinesFromTextFile("anAuthor.txt"),1);
        topologyBuilder.setBolt("PROCESS_AUTHORS",new ProcessAuthors(),1).shuffleGrouping("READ_AUTHORS_MONGO");

     //   topologyBuilder.setSpout("READ_TWEETS_FROM_MONGODB",new ReadLinesFromTextFile("tweets.txt"),1);
        topologyBuilder.setSpout("READ_TWEETS_FROM_MONGODB",new ReadLinesFromTextFile("tweets.txt"),1);

        topologyBuilder.setBolt("PROCESS_TWEETS_FROM_MONGODB",new FromTweetsFileToFurtherProcessing(),1).shuffleGrouping("READ_TWEETS_FROM_MONGODB");

      // topologyBuilder.setBolt("SEARCH",new SingleJoinBolt(new Fields("USERNAME","ID","TWEET","DATE","IN_REPLY_TO","FOLLOWERS","FRIENDS")),4).fieldsGrouping("PROCESS_AUTHORS", new Fields("USERNAME")).fieldsGrouping("PROCESS_TWEETS_FROM_MONGODB",new Fields("USERNAME"));

        topologyBuilder.setBolt("SEARCH_1",new SearchForTheAuthorsTweetData(),16).fieldsGrouping("PROCESS_AUTHORS", new Fields("USERNAME")).fieldsGrouping("PROCESS_TWEETS_FROM_MONGODB", new Fields("USERNAME"));




   //     topologyBuilder.setBolt("JOIN",new FileWriter("join.txt")).shuffleGrouping("SEARCH");
        //topologyBuilder.setBolt("WRITE_AUTHORS_DATA_TO_MONGODB",new WriteTheAuthorsTweetFeedsToMongoDB()).shuffleGrouping("SEARCH");



        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(1*60*60*1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }


}
