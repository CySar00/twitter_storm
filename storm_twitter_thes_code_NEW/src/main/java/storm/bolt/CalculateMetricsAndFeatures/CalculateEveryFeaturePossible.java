package storm.bolt.CalculateMetricsAndFeatures;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 3/28/15.
 */
public class CalculateEveryFeaturePossible extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,Double>TS=new HashMap<String, Double>();
    Map<String,Double>SS=new HashMap<String, Double>();
    Map<String,Double>NON_CS=new HashMap<String, Double>();
    Map<String,Double>RI=new HashMap<String, Double>();
    Map<String,Double>MI=new HashMap<String, Double>();
    Map<String,Double>ID=new HashMap<String, Double>();
    Map<String,Double>ID1=new HashMap<String, Double>();
    Map<String,Double>NS=new HashMap<String, Double>();

    private static final double l=0.05;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","OT1","OT2","OT3","OT4","CT1","CT2","RT1","RT2","RT3",
                "M1","M2","M3","M4","G1","G2","G3","G4","FREQUENCY","MORNING_COUNT","NOON_COUNT",
                "AFTERNOON_COUNT","EVENING_COUNT","NIGHT_COUNT","TS","SS","NON_CS","RI","MI","ID","ID1","NS"));


    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);

        int NumberOfTweets=(int)input.getInteger(1);

        Double OT1=input.getDouble(2);
        Double OT2=input.getDouble(3);
        Double OT3=input.getDouble(4);
        Double OT4=input.getDouble(5);

        Double CT1=input.getDouble(6);
        Double CT2=input.getDouble(7);

        Double RT1=input.getDouble(8);
        Double RT2=input.getDouble(9);
        Double RT3=input.getDouble(10);

        Double M1=input.getDouble(11);
        Double M2=input.getDouble(12);
        Double M3=input.getDouble(13);
        Double M4=input.getDouble(14);

        Double G1=input.getDouble(15);
        Double G2=input.getDouble(16);
        Double G3=input.getDouble(17);
        Double G4=input.getDouble(18);

        Double FREQUENCY=input.getDouble(19);
        Double MORNING_COUNT=input.getDouble(20);
        Double NOON_COUNT=input.getDouble(21);
        Double AFTERNOON_COUNT=input.getDouble(22);
        Double EVENING_COUNT=input.getDouble(23);
        Double NIGHT_COUNT=input.getDouble(24);

        Double TS=this.TS.get(author);
        if(TS==null)
            TS=0.0;

        Double SS=this.SS.get(author);
        if(SS==null)
            SS=0.0;

        Double NON_CS=this.NON_CS.get(author);
        if(NON_CS==null)
            NON_CS=0.0;

        Double RI=this.RI.get(author);
        if(RI==null)
            RI=0.0;

        Double MI=this.MI.get(author);
        if(MI==null)
            MI=0.0;

        Double ID=this.ID.get(author);
        if(ID==null)
            ID=0.0;

        Double ID1=this.ID1.get(author);
        if(ID1==null)
            ID1=0.0;

        Double NS=this.NS.get(author);
        if(NS==null)
            NS=0.0;

        TS=(OT1+CT1+RT1)/NumberOfTweets;

        if(OT1+RT1!=0) {
            SS = OT1 / (OT1 + RT1);
        }else {
            SS=0.0;
        }

        if(OT1+CT1!=0) {
            NON_CS = OT1 / (OT1 + CT1) + l * (CT1 - CT2) / (CT1 + 1);
        }else{
            NON_CS=0.0;
        }

        RI=RT2*Math.log(RT3+1);

        MI=M3*Math.log(M4+1)-M1*Math.log(M2+1);

        ID=Math.log(G3+1)-Math.log(G4+1);

        ID1=Math.log((G3+1)/(G1+1))-Math.log((G4+1)/(G2+1));

        NS=Math.log(G1+1)-Math.log(G2+1);

        if(!this.TS.containsKey(author) && !this.SS.containsKey(author) && !this.NON_CS.containsKey(author) && !this.RI.containsKey(author) && !this.MI.containsKey(author) && !this.ID.containsKey(author) && !this.ID1.containsKey(author) && !this.NS.containsKey(author)){

            collector.emit(new Values(author,OT1,OT2,OT3,OT4,CT1,CT2,RT1,RT2,RT3,M1,M2,M3,M4,G1,G2,G3,G4,FREQUENCY,MORNING_COUNT,NOON_COUNT,AFTERNOON_COUNT,EVENING_COUNT,NIGHT_COUNT,TS,SS,NON_CS,RI,MI,ID,ID1,NS));

            this.TS.put(author,TS);
            this.SS.put(author,SS);
            this.NON_CS.put(author,NON_CS);
            this.RI.put(author,RI);
            this.MI.put(author,MI);
            this.ID.put(author,ID);
            this.ID1.put(author,ID1);
            this.NS.put(author,NS);

        }

    }
}
