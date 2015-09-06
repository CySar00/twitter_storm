package Storm.Bolts.CreatingTheDataSet.UsersAndAuthors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/23/15.
 */
public class WriteAuthorsFromTextFileToMongoDB extends BaseRichBolt {
    private OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("AUTHOR"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);

        System.out.println(line);
        int index1=line.indexOf("[[");
        int index2=line.indexOf("]]");

        String substring="";
        substring=line.substring(index1+2,index2);
        System.out.println(substring);

        String[] split=substring.split(",");
        List<String>list=new ArrayList<String>();
        for(int i=0; i<split.length;i++){
            list.add(split[i]);
        }
        System.out.println(list);

        StringBuilder builder=new StringBuilder();
        builder.append(list.get(0));
        for(int i=1;i<list.size();i++){
            builder.append(",").append(list.get(i));
        }

        String str=builder.toString();
       // System.out.println(str);

        Storm.Databases.CassandraDB.Preprocessing.CassandraSchemaForAuthors.writeAuthorsToCassandra(substring);

        System.out.println(Storm.Databases.CassandraDB.Preprocessing.CassandraSchemaForAuthors.readAuthorsFromCassandraDB());













    }



}
