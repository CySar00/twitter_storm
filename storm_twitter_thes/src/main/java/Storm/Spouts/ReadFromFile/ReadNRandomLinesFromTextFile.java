package Storm.Spouts.ReadFromFile;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 7/17/15.
 */
public class ReadNRandomLinesFromTextFile extends BaseRichSpout {
    private final String fileName;
    int n;

    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;

    private List<String> lines=new ArrayList<String>();
    private List<String>tempLines=new ArrayList<String>();

    public ReadNRandomLinesFromTextFile(String fileName,int n){
        this.fileName=fileName;
        this.n=n;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        linesRead=new AtomicLong(0);
        this.collector=collector;
        try{
            bufferedReader=new BufferedReader(new FileReader(fileName));
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void deactivate() {
        try{
            bufferedReader.close();
        }catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try{
            String line=bufferedReader.readLine();
            String lastLine;
            if(line!=null){
                long ID=linesRead.incrementAndGet();
                lines.add(line);
            }else {
                tempLines=selectNLinesFromTextFile(lines,this.n);
                int i=0;
                for(String tempLine:tempLines){
                    collector.emit(new Values(i,tempLine));
                    i++;
                }
                Thread.sleep(100000);
            }
        }catch (Exception ex){
                ex.printStackTrace();
            }
        }

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {
        System.out.print("Failed line number "+msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("INDEX","LINE"));
    }

    private List<String>selectNLinesFromTextFile(List<String>lines,int n){
        List<String>copy=new LinkedList<String>(lines);
        Collections.shuffle(copy);
        return  copy.subList(0,n);
    }


}

