package storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.Clusters.databases.ClusterDatabase;

import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 6/19/15.
 */
public class EmitDataPoints extends BaseRichSpout {
    SpoutOutputCollector collector;
    Integer count=0;
    Random random=new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String serMatrices[];
        int [][][]matrices={{{1,2,3},{4,5,6}},{{3,4,5},{6,7,8}}};


        serMatrices= ClusterDatabase.getSerialized2DMatrices();
        int randomInteger=random.nextInt();
        int randomModulo=randomInteger%4;

        if (randomModulo >= 0 && randomModulo <= 3) {
            if (serMatrices != null) {

                int[][]matrix=matrices[randomModulo];
                if(matrix!=null){
                    System.out.println(matrix);
                }
                EmitMatrixWithClosestCluster.emitMatrixWithClosestClusterCenter(this.collector, serMatrices, matrices[randomModulo]);

            }
        }



    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("index","matrix"));
    }
}
