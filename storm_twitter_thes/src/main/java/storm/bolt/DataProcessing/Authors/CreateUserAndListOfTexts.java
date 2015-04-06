package storm.bolt.DataProcessing.Authors;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 3/27/15.
 */
public class CreateUserAndListOfTexts extends BaseRichBolt {
    private OutputCollector collector;

    String anAuthor;

    String user;String text;

    Map<String,List<String>>userAndTexts=new HashMap<String, List<String>>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","TEXTS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String sourceComponent = input.getSourceComponent();
        HashSet<String> authors = new HashSet<String>();


        if (sourceComponent.equals("CREATE_THE_FUCKING_AUTHORS")) {
            anAuthor = input.getString(0);
        }

        if (sourceComponent.equals("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES")) {
            user = input.getString(0);
            text = input.getString(2);
        }


        if (anAuthor != null && user != null) {
            if (user.equals(anAuthor)) {
                if (!authors.contains(user)) {

                    String author = user;

                    List<String> texts = userAndTexts.get(author);
                    if (texts == null)
                        texts = new ArrayList<String>();
                    texts.add(text);

                    List<String> uniqueTexts = new ArrayList<String>();
                    if (!uniqueTexts.contains(text)) {
                        uniqueTexts.add(text);
                    }

                    if (!userAndTexts.containsKey(author)) {
                        System.out.println(author + " " + uniqueTexts);

                        collector.emit(new Values(author, uniqueTexts));
                        userAndTexts.put(author, uniqueTexts);
                        authors.add(author);
                    }
                }
            }
        }
        collector.ack(input);

    }
}