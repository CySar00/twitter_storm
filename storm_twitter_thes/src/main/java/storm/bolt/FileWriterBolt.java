package storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by christina on 10/7/14.
 */
public class FileWriterBolt extends BaseRichBolt {
    PrintWriter writer;
    int count=0;

    private OutputCollector collector;
    private String filename;

    public  FileWriterBolt(String filename){
        this.filename=filename;
    }

    @Override
    public void cleanup(){
        writer.close();
        super.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){

    }

    @Override
    public void prepare(Map map,TopologyContext context,OutputCollector collector){
      this.collector=collector;

        try{
         writer=new PrintWriter(filename,"UTF-8");
        }catch (FileNotFoundException ex){
            ex.printStackTrace();
        }catch (UnsupportedEncodingException ex){
            ex.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple){
        writer.println((count++)+":"+tuple);
        writer.flush();
        //confirm that the tuple has been treated
        collector.ack(tuple);
    }




}
