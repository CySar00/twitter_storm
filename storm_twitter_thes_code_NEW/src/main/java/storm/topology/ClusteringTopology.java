package storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.Clusters.ClusterDataPoints;
import storm.bolt.Clusters.databases.ClusterDatabase;
import storm.bolt.Clusters.databases.SerializerAndDeserializer;
import storm.bolt.FileWriterBolt;
import storm.spout.EmitDataPoints;

/**
 * Created by christina on 6/18/15.
 */
public class ClusteringTopology {

    public static void initializeDB(){
        int matrices[][][]={{{1,2,3},{1,5,6}},
                                {{1,3,4},{5,7,8}}};
        String serMatrix="";
        for(int i=0;i<matrices.length;i++){
            serMatrix= SerializerAndDeserializer.serializer(matrices[i]);
            ClusterDatabase.setSerialized2DMatrix(i+1,serMatrix);
        }
    }

    public static void main(String[]args)throws Exception{
        initializeDB();
        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout("EmitMatrices",new EmitDataPoints(),2);
        //builder.setBolt("createClusters",new ClusterDataPoints(),4).fieldsGrouping("EmitMatrices",new Fields("index"));
        //builder.setBolt("writeClustersToFile",new FileWriterBolt("clusters.txt")).shuffleGrouping("createClusters");

        Config config=new Config();
        config.setDebug( true);

        if(args!=null && args.length>0){
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else{
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("test",config,builder.createTopology());
            Utils.sleep(9000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

    }


}
