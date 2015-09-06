package storm.topology.DataAndUsers;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.CalculateMetricsAndFeatures.CalculateEveryFeaturePossible;
import storm.bolt.CalculateMetricsAndFeatures.CalculateEveryMetricPossible;
import storm.bolt.CalculateMetricsAndFeatures.EmitAllMetricsAndFeaturesPerUserIntoListsAndVectors;
import storm.bolt.DataProcessing.Authors.CreateListOfAuthors;
import storm.bolt.DataProcessing.Authors.CreateUserAndListOfTexts;
import storm.bolt.DataProcessing.Authors.createUserAndListOfTweets_NEW;
import storm.bolt.DataProcessing.MapUsersFromDataSet;
import storm.bolt.DataProcessing.SplitLinesIntoTupleValueFields;
import storm.bolt.DataProcessing.UserAndListOfTweets;
import storm.bolt.FileWriterBolt;
import storm.spout.LinesSpout;
import storm.spout.ReadLastLineSpout;

/**
 * Created by christina on 6/21/15.
 */
public class UserAndTweetsTopology {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MONGODB_DATA",new LinesSpout("tweets.txt"),4);
        topologyBuilder.setBolt("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES",new SplitLinesIntoTupleValueFields(),256).shuffleGrouping("READ_MONGODB_DATA");

        topologyBuilder.setSpout("READ_THE_AUTHORS", new ReadLastLineSpout("theUsers.txt"));
        topologyBuilder.setBolt("CREATE_THE_AUTHORS",new CreateListOfAuthors(),256).shuffleGrouping("READ_THE_AUTHORS");

        topologyBuilder.setBolt("SEARCH_TWO_BOLTS",new MapUsersFromDataSet(),256).fieldsGrouping("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES",new Fields("USERNAME")).fieldsGrouping("CREATE_THE_AUTHORS",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_USER_PER_TWEET_TO_FILE",new FileWriterBolt("userAndTweet.txt")).shuffleGrouping("SEARCH_TWO_BOLTS");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(6*60*60*10000000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }


}
