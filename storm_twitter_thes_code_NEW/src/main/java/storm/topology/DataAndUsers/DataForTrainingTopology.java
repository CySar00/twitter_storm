package storm.topology.DataAndUsers;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.DataProcessing.Authors.CreateListOfAuthors;
import storm.bolt.DataProcessing.MapUsersFromDataSet;
import storm.bolt.DataProcessing.SplitLinesIntoTupleValueFields;
import storm.bolt.FileWriterBolt;
import storm.spout.LinesSpout;
import storm.spout.ReadLastLineSpout;

/**
 * Created by christina on 6/30/15.
 */
public class DataForTrainingTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_TWEETS_AND_ETC_INFORMATION",new LinesSpout("userAndTweet.txt"),4);


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(6 * 60 * 60 * 10000000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
