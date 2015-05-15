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
import storm.bolt.DataProcessing.SplitLinesIntoTupleValueFields;
import storm.bolt.FileWriterBolt;
import storm.spout.LinesSpout;
import storm.spout.ReadLastLineSpout;

/**
 * Created by christina on 3/27/15.
 */
public class FeaturesAndMetricsTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MONGODB_DATA",new LinesSpout("mongoData.txt"),4);
        topologyBuilder.setBolt("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES",new SplitLinesIntoTupleValueFields(),8).shuffleGrouping("READ_MONGODB_DATA");

        topologyBuilder.setSpout("READ_THE_FUCKING_AUTHORS", new ReadLastLineSpout("theUsers.txt"));
        topologyBuilder.setBolt("CREATE_THE_FUCKING_AUTHORS",new CreateListOfAuthors(),4).shuffleGrouping("READ_THE_FUCKING_AUTHORS");

        topologyBuilder.setBolt("CREATE_USERNAME_AND_LIST_OF_TEXTS",new CreateUserAndListOfTexts(),32).fieldsGrouping("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES",new Fields("USERNAME")).fieldsGrouping("CREATE_THE_FUCKING_AUTHORS",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_USER_AND_TEXTS_TO_FILE",new FileWriterBolt("userAndTweets.txt")).shuffleGrouping("CREATE_USERNAME_AND_LIST_OF_TEXTS");


   /*     topologyBuilder.setBolt("CALCULATE_EVERY_METRIC_POSSIBLE",new CalculateEveryMetricPossible(),32).setNumTasks(16).fieldsGrouping("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES",new Fields("USERNAME")).fieldsGrouping("CREATE_THE_FUCKING_AUTHORS",new Fields("USERNAME"));
        topologyBuilder.setBolt("CALCULATE_EVERY_FEATURE_POSSIBLE", new CalculateEveryFeaturePossible(), 32).fieldsGrouping("CALCULATE_EVERY_METRIC_POSSIBLE",new Fields("USERNAME"));

        topologyBuilder.setBolt("EMIT_ALL_METRICS_AND_FEATURES_INTO_LIST_AND_VECTOR",new EmitAllMetricsAndFeaturesPerUserIntoListsAndVectors()).globalGrouping("CALCULATE_EVERY_FEATURE_POSSIBLE");
        topologyBuilder.setBolt("WRITE_TO_FILE",new FileWriterBolt("data.txt")).shuffleGrouping("EMIT_ALL_METRICS_AND_FEATURES_INTO_LIST_AND_VECTOR");
    */

        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(60*100000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }



}
