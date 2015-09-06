package Storm.Bolts.Databases.MongoDB;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by christina on 7/24/15.
 */
public class WriteTheSelectedAuthorsToMongoDB extends BaseRichBolt {


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {


    }

    @Override
    public void execute(Tuple input) {
        try{
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            DB db=mongoClient.getDB("storm_NEW_");
            final DBCollection collection=db.getCollection("authors");

            String author=input.getString(0);
            DBObject dbObject=new BasicDBObject();
            dbObject.put("AUTHOR",author);
            collection.insert(dbObject);

            Runtime runtime = Runtime.getRuntime();
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
            Date date = new Date();
            String xyz = dateFormat.format(date);

            String query = "mongoexport --db storm_NEW_ --collection authors --fields AUTHOR -o anAuthor.txt";
            try {
                Process process = runtime.exec(query);
                System.out.println(query);
                System.out.println(xyz);

            } catch (IOException ex) {
                ex.printStackTrace();

            }
            mongoClient.close();

        }catch (UnknownHostException ex) {
            ex.printStackTrace();

        }
    }
}
