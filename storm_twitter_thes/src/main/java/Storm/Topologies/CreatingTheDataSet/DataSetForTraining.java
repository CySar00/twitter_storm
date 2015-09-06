package Storm.Topologies.CreatingTheDataSet;

import Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData.FromMongoDBToFurtherProcessing;
import Storm.Bolts.FileWriter;
import Storm.Bolts.Preprocessing.CreateTheTrainingDataSet;
import Storm.Bolts.Preprocessing.RemoveAnyDuplicateOrMoreUsersForTraining;
import Storm.Spouts.ReadFromFile.ReadLinesFromTextFile;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/12/15.
 */
public class DataSetForTraining {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_AUTHORS_TWEET_DATA_FROM_MONGODB", new ReadLinesFromTextFile("author_and_tweet_data.txt"),1);
        topologyBuilder.setBolt("PROCESS_AUTHORS_TWEET_DATA_FROM_MONGODB",new FromMongoDBToFurtherProcessing(),8).shuffleGrouping("READ_AUTHORS_TWEET_DATA_FROM_MONGODB");

        topologyBuilder.setBolt("CREATE_TRAINING_DATA_SET",new CreateTheTrainingDataSet()).fieldsGrouping("PROCESS_AUTHORS_TWEET_DATA_FROM_MONGODB",new Fields("USERNAME"));
        topologyBuilder.setBolt("REMOVE_ANY_DUPLICATE_VALUES",new RemoveAnyDuplicateOrMoreUsersForTraining()).fieldsGrouping("CREATE_TRAINING_DATA_SET",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_TRAINING_DATA_SET_TO_FILE",new FileWriter("author_and_tweets.txt")).shuffleGrouping("REMOVE_ANY_DUPLICATE_VALUES");

        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(10*60 * 60 * 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
