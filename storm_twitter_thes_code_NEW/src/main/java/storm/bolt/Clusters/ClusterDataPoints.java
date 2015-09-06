package storm.bolt.Clusters;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.bolt.Clusters.databases.ClusterDatabase;
import storm.bolt.Clusters.databases.SerializerAndDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 6/19/15.
 */
public class ClusterDataPoints extends BaseRichBolt {

    OutputCollector collector;
    HashMap<Integer,int[][]>matricesHashmaps=new HashMap<Integer, int[][]>();
    Map<String,Integer>counts=new HashMap<String, Integer>();
    int []n=new int[30];

    public void getInitialMatricesFromDB(){
        String[]serMatricesFromDB= ClusterDatabase.getSerialized2DMatrices();
        if(serMatricesFromDB!=null){
            for(int i=0;i<serMatricesFromDB.length;i++) {
                try {
                    this.matricesHashmaps.put(i + 1, SerializerAndDeserializer.deserialize(serMatricesFromDB[i]));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("index","matrix"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        getInitialMatricesFromDB();
    }

    public void updateDB(int index,int[][]matrixToUpdate){
        String matrixSerial=SerializerAndDeserializer.serializer(matrixToUpdate);
        ClusterDatabase.setSerialized2DMatrix(index,matrixSerial);
    }

    @Override
    public void execute(Tuple input) {
        int index=(int)input.getInteger(0);
        int resultMatrix[][]=new int[2][];
        int[][] matrix=(int[][])input.getValue(1);
        int[][]centroidMatrix=matricesHashmaps.get(index);

        if(centroidMatrix!=null && matrix!=null){
            n[index]+=1;
            resultMatrix=MatrixOperations.addMatrices(matrix,centroidMatrix,n[index]);
            matricesHashmaps.put(index,resultMatrix);
            updateDB(index,resultMatrix);
        }
        collector.emit(new Values(index,resultMatrix));
        System.out.println(index+" "+resultMatrix);
      //  collector.ack(input);
    }
}
