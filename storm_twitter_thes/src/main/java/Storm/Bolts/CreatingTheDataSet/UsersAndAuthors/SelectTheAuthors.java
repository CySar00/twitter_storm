package Storm.Bolts.CreatingTheDataSet.UsersAndAuthors;

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
 * Created by christina on 7/10/15.
 */
public class SelectTheAuthors extends BaseBasicBolt {
    private static final int AUTHORS=500;

    private Buffer buffer=new CircularFifoBuffer();

    private List<String>listOfUsers=new CopyOnWriteArrayList<String>();
    private Random random=new Random();
    List<String>authors=new ArrayList<String>();
    Set<String>unique=new HashSet<String>();


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        buffer.add(input);
        create();

        if(!authors.isEmpty()) {
            collector.emit(new Values(authors));
        }

    }

    private void create(){
        Iterator iterator=buffer.iterator();
        while (iterator.hasNext()){
            Tuple tuple=(Tuple)iterator.next();

            String author=tuple.getString(0);
            if(!unique.contains(author)) {
                listOfUsers.add(author);
                unique.add(author);
            }
        }
        System.out.println(listOfUsers);

        if(!listOfUsers.isEmpty() && listOfUsers.size()>=100){
            authors=selectRandomUsers(listOfUsers,100);
        }
    }

    public  List<String>selectRandomUsers(List<String>list,int n){
        List<String>copyOfListOfUsers=new LinkedList<String>(list);
        Collections.shuffle(copyOfListOfUsers);

         List<String>temp=new ArrayList<String>();
         temp=copyOfListOfUsers.subList(0,n);
         return temp;

          }


}
