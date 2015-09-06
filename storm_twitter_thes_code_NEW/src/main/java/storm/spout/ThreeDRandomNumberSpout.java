package storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by christina on 4/2/15.
 */
public class ThreeDRandomNumberSpout extends BaseRichSpout {
    public static final int TUPLES=10;
    public static final int CLUSTERS=3;
    public static final int FEATURES=31;

    double[][][]membershipArray=new double[TUPLES][CLUSTERS][FEATURES];

    SpoutOutputCollector collector;

    Random random=new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        create3DMembershipArray();
        for(int i=0;i<TUPLES;i++){
            for(int j=0;j<CLUSTERS;j++){
                collector.emit(new Values(membershipArray[i][j]));
              //  System.out.println(membershipArray[i][j]);

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
        declarer.declare(new Fields("VECTOR"));

    }

    private  void create3DMembershipArray(){
        for(int i=0;i<TUPLES;i++){
            for (int j=0;j<CLUSTERS;j++){
                for(int k=0;k<FEATURES;k++){
                    membershipArray[i][j][k]=random.nextDouble()+0.05;
                }
            }
        }

    }

}
