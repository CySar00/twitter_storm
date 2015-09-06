package storm.topology.DataAndUsers;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import storm.bolt.DataProcessing.Authors.SelectTheAuthors;
import storm.bolt.DataProcessing.SplitLinesIntoTupleValueFields;
import storm.bolt.FileWriterBolt;

import storm.spout.LinesSpout;

/**
 * Created by christina on 3/25/15.
 */
public class UsersTopology {

    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MONGODB_DATA",new LinesSpout("tweets.txt"));
        topologyBuilder.setBolt("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES",new SplitLinesIntoTupleValueFields(),64).shuffleGrouping("READ_MONGODB_DATA");

        topologyBuilder.setBolt("SELECT_THE_AUTHORS",new SelectTheAuthors()).globalGrouping("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES");
        topologyBuilder.setBolt("WRITE_THE_SELECTED_AUTHORS_TO_TEXT_FILE",new FileWriterBolt("theUsers.txt")).shuffleGrouping("SELECT_THE_AUTHORS");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(100000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }



}
