package Storm.Bolts.ClusteringTechniques.ClassicClustering.KMeans.CreatingTheInitialCentroids;

import Storm.Databases.CassandraDB.Clustering.KCentroidsDatabase;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class CreateTheInitialKCentroids extends BaseRichBolt {


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        Integer index=input.getInteger(0);
        List<Double>values=(List<Double>)input.getValue(1);
        //System.out.println(values.size());

        String serialized="";
        serialized=Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.serializeJavaDoublesList(values);

        KCentroidsDatabase.setSerializedMap(index,serialized);

    }
}
