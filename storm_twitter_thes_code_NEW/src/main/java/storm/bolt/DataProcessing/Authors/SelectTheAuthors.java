package storm.bolt.DataProcessing.Authors;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by christina on 3/25/15.
 */
public class SelectTheAuthors extends BaseBasicBolt {
    public static final int TUPLES=200;
    public static final int AUTHORS=100;

    Buffer buffer=new CircularFifoBuffer(TUPLES);
    private List<String> users=new CopyOnWriteArrayList<String>();
    private List<String> authors=new CopyOnWriteArrayList<String>();
    private List<String>selectedAuthors=new ArrayList<String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHORS"));

    }


    @Override
    public void execute(Tuple input,BasicOutputCollector collector) {
        HashSet<Tuple> set = new HashSet<Tuple>();
        if (!set.contains(input)) {
            buffer.add(input);
            set.add(input);
        }
        createListOfTheFuckingAuthors();
        if(!authors.isEmpty() && authors.size()==TUPLES){
            selectedAuthors=pickRandomAuthors(authors,AUTHORS);

            System.out.println(selectedAuthors+" "+selectedAuthors.size());
            collector.emit(new Values(selectedAuthors));
        }
    }

    private void createListOfTheFuckingAuthors(){
        Iterator iterator=buffer.iterator();
        HashSet<String>users=new HashSet<String>();

        while (iterator.hasNext()){
            Tuple tuple=(Tuple)iterator.next();

                String username=tuple.getString(0);
                if(!users.contains(username) && !authors.contains(username)){
                    authors.add(username);
                    users.add(username);

                    if(users.size()==TUPLES){
                        break;
                    }
                }
        }
    }

    private List<String> pickRandomAuthors(List<String>list,int n){
        List<String>copy=new LinkedList<String>(list);
        Collections.shuffle(copy);
        return copy.subList(0,n);
    }


}
