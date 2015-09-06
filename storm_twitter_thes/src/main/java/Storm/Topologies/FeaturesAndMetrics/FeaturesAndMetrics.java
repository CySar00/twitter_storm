package Storm.Topologies.FeaturesAndMetrics;

import Storm.Bolts.CreatingTheDataSet.ProcessingAuthorsAndTweetData.FromMongoDBToFurtherProcessing;

import Storm.Bolts.FeaturesAndMetrics.CreateHelpMaps.CreateThreeHelpHashmaps;
import Storm.Bolts.FeaturesAndMetrics.Features.Features;
import Storm.Bolts.FeaturesAndMetrics.GatherMostTweetDataForAuthors;
import Storm.Bolts.FeaturesAndMetrics.Metrics.Metrics;

import Storm.Bolts.FeaturesAndMetrics.RemoveAnyDuplicateValuesOfAuthors;
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
 * Created by christina on 7/16/15.
 */

public class FeaturesAndMetrics {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_AUTHORS_DATA_FROM_MONGODB",new ReadLinesFromTextFile("author_and_tweet_data.txt"),1);
        topologyBuilder.setBolt("PROCESS_AUTHOR_DATA_FROM_MONGODB",new FromMongoDBToFurtherProcessing(),16).shuffleGrouping("READ_AUTHORS_DATA_FROM_MONGODB");

        topologyBuilder.setBolt("GATHER_MOST_TWEET_DATA_OF_THE_AUTHORS", new GatherMostTweetDataForAuthors()).fieldsGrouping("PROCESS_AUTHOR_DATA_FROM_MONGODB",new Fields("USERNAME"));
        topologyBuilder.setBolt("REMOVE_ANY_DUPLICATE_VALUES", new RemoveAnyDuplicateValuesOfAuthors()).fieldsGrouping("GATHER_MOST_TWEET_DATA_OF_THE_AUTHORS", new Fields("USERNAME"));

        topologyBuilder.setBolt("CREATE_THREE_HELP_MAPS",new CreateThreeHelpHashmaps()).fieldsGrouping("REMOVE_ANY_DUPLICATE_VALUES",new Fields("USERNAME"));
        topologyBuilder.setBolt("JOIN_1",new SingleJoinBolt(new Fields("USERNAME","IDS","TWEETS","DATES","IN_REPLY_TOS","FOLLOWERS","FRIENDS","MAP_OF_IDS","MAP_OF_TWEETS","MAP_OF_DATES"))).fieldsGrouping("REMOVE_ANY_DUPLICATE_VALUES",new Fields("USERNAME")).fieldsGrouping("CREATE_THREE_HELP_MAPS",new Fields("USERNAME"));


        topologyBuilder.setBolt("METRICS",new Metrics()).fieldsGrouping("JOIN_1", new Fields("USERNAME"));
        topologyBuilder.setBolt("FEATURES",new Features()).fieldsGrouping("METRICS",new Fields("USERNAME"));


        topologyBuilder.setBolt("WRITE_METRICS_AND_FEATURES_TO_TEXT_FILE",new FileWriter("metrics_and_features.txt")).shuffleGrouping("FEATURES");




        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep( 60 *10 * 1000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
