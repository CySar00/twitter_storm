package Storm.Bolts.FeaturesAndMetrics.Features;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/26/15.
 */
public class Features extends BaseRichBolt {

    private  OutputCollector collector;

    private static final double lamda=0.05;


    Map<String,List<String>>tweets;
    Map<String,List<Long>>IDs;
    Map<String ,List<String>>users;
    Map<String,List<Date>>dates;
    Map<String,List<String>>followers;
    Map<String,List<String>>friends;

    Map<String,Double>numberOfTweets;
    Map<String,Double>OT1;
    Map<String,Double>OT2;
    Map<String,Double>OT3;
    Map<String,Double>OT4;

    Map<String,Double>CT1;
    Map<String,Double>CT2;

    Map<String,Double>RT1;
    Map<String,Double>RT2;
    Map<String,Double>RT3;

    Map<String,Double>M1;
    Map<String,Double>M2;
    Map<String,Double>M3;
    Map<String,Double>M4;

    Map<String,Double>G1;
    Map<String,Double>G2;
    Map<String,Double>G3;
    Map<String,Double>G4;

    Map<String,Double>FREQUENCY;
    Map<String,Double>MORNING_COUNT;
    Map<String,Double>NOON_COUNT;
    Map<String,Double>AFTERNOON_COUNT;
    Map<String ,Double>EVENING_COUNT;
    Map<String ,Double>NIGHT_COUNT;

    Map<String,Double>TS;
    Map<String,Double>SS;
    Map<String,Double>NON_CS;
    Map<String ,Double>RI;
    Map<String ,Double>MI;
    Map<String,Double>ID;
    Map<String ,Double>ID1;
    Map<String ,Double>NS;



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","NUMBER_OF_TWEETS", "OT1","OT2","OT3","OT4", "CT1","CT2", "RT1","RT2","RT3", "M1","M2","M3","M4", "G1","G2","G3","G4", "FREQUENCY","MORNING_COUNT","NOON_COUNT","AFTERNOON_COUNT","EVENING_COUNT","NIGHT_COUNT","TS","SS","NON_CS","RI","MI","ID","ID1","NS"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        numberOfTweets=new HashMap<String, Double>();
        OT1=new HashMap<String, Double>();
        OT2=new HashMap<String, Double>();
        OT3=new HashMap<String, Double>();
        OT4=new HashMap<String, Double>();

        CT1=new HashMap<String, Double>();
        CT2=new HashMap<String, Double>();

        RT1=new HashMap<String, Double>();
        RT2=new HashMap<String, Double>();
        RT3=new HashMap<String, Double>();

        M1=new HashMap<String, Double>();
        M2=new HashMap<String, Double>();
        M3=new HashMap<String, Double>();
        M4=new HashMap<String, Double>();

        G1=new HashMap<String, Double>();
        G2=new HashMap<String, Double>();
        G3=new HashMap<String, Double>();
        G4=new HashMap<String, Double>();

        FREQUENCY=new HashMap<String, Double>();

        MORNING_COUNT=new HashMap<String, Double>();
        NOON_COUNT=new HashMap<String, Double>();
        AFTERNOON_COUNT=new HashMap<String, Double>();
        EVENING_COUNT=new HashMap<String, Double>();
        NIGHT_COUNT=new HashMap<String, Double>();

        TS=new HashMap<String, Double>();
        SS=new HashMap<String, Double>();
        NON_CS=new HashMap<String, Double>();
        RI=new HashMap<String, Double>();
        MI=new HashMap<String, Double>();
        ID=new HashMap<String, Double>();
        ID1=new HashMap<String, Double>();
        NS=new HashMap<String, Double>();
    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);

        Double number_of_tweets=(double)input.getInteger(1);

        Double ot1=(double)input.getInteger(2);
        Double ot2=(double)input.getInteger(3);
        Double ot3= input.getDouble(4);
        Double ot4=(double)input.getInteger(5);

        Double ct1=(double)input.getInteger(6);
        Double ct2=(double)input.getInteger(7);

        Double rt1=(double)input.getInteger(8);
        Double rt2=(double)input.getInteger(9);
        Double rt3=(double)input.getInteger(10);

        Double m1=(double)input.getInteger(11);
        Double m2=(double)input.getInteger(12);
        Double m3=(double)input.getInteger(13);
        Double m4=(double)input.getInteger(14);

        Double g1=(double)input.getInteger(15);
        Double g2=(double)input.getInteger(16);
        Double g3=(double)input.getInteger(17);
        Double g4=(double)input.getInteger(18);

        Double frequency =input.getDouble(19);
        Double morning=(double)input.getInteger(20);
        Double noon=(double)input.getInteger(21);
        Double afternoon=(double)input.getInteger(22);
        Double evening=(double)input.getInteger(23);
        Double night=(double)input.getInteger(24);

        Double numberOfTweets=this.numberOfTweets.get(author);
        if(numberOfTweets==null){
            numberOfTweets=0.0;
        }
        numberOfTweets=number_of_tweets;
        this.numberOfTweets.put(author,numberOfTweets);

        Double OT1=this.OT1.get(author);
        if(OT1==null){
            OT1=0.0;
        }
        OT1=ot1;
        this.OT1.put(author,OT1);

        Double OT2=this.OT2.get(author);
        if(OT2==null){
            OT2=0.0;
        }
        OT2=ot2;
        this.OT2.put(author,OT2);

        Double OT3=this.OT3.get(author);
        if(OT3==null){
            OT3=0.0;
        }
        OT3=ot3;
        this.OT3.put(author,OT3);

        Double OT4=this.OT4.get(author);
        if(OT4==null){
            OT4=0.0;
        }
        OT4=ot4;
        this.OT4.put(author,OT4);


        Double CT1=this.CT1.get(author);
        if(CT1==null){
            CT1=0.0;
        }
        CT1=ct1;
        this.CT1.put(author,CT1);

        Double CT2=this.CT2.get(author);
        if(CT2==null){
            CT2=0.0;
        }
        CT2=ct2;
        this.CT2.put(author,CT2);

        Double RT1=this.RT1.get(author);
        if(RT1==null){
            RT1=0.0;
        }
        RT1=rt1;
        this.RT1.put(author,RT1);

        Double RT2=this.RT2.get(author);
        if(RT2==null){
            RT2=0.0;
        }
        RT2=rt2;
        this.RT2.put(author,RT2);

        Double RT3=this.RT3.get(author);
        if(RT3==null){
            RT3=0.0;
        }
        RT3=rt3;
        this.RT3.put(author,RT3);

        Double M1=this.M1.get(author);
        if(M1==null){
            M1=0.0;
        }
        M1=m1;
        this.M1.put(author,M1);

        Double M2=this.M2.get(author);
        if(M2==null){
            M2=0.0;
        }
        M2=m2;
        this.M2.put(author,M2);

        Double M3=this.M3.get(author);
        if(M3==null){
            M3=0.0;
        }
        M3=m3;
        this.M3.put(author,M3);

        Double M4=this.M4.get(author);
        if(M4==null){
            M4=0.0;
        }
        M4=m4;
        this.M4.put(author,M4);

        Double G1=this.G1.get(author);
        if(G1==null){
            G1=0.0;
        }
        G1=g1;
        this.G1.put(author,g1);

        Double G2=this.G2.get(author);
        if(G2==null){
            G2=0.0;
        }
        G2=g2;
        this.G2.put(author,g2);

        Double G3=this.G3.get(author);
        if(G3==null){
            G3=0.0;
        }
        G3=g3;
        this.G3.put(author,g3);

        Double G4=this.G4.get(author);
        if(G4==null){
            G4=0.0;
        }
        G4=g4;
        this.G4.put(author,G4);

        Double FREQUENCY=this.FREQUENCY.get(author);
        if(FREQUENCY==null){
            FREQUENCY=0.0;
        }
        FREQUENCY=frequency;
        this.FREQUENCY.put(author,FREQUENCY);

        Double MORNING_COUNT=this.MORNING_COUNT.get(author);
        if(MORNING_COUNT==null){
            MORNING_COUNT=0.0;
        }
        MORNING_COUNT=morning;
        this.MORNING_COUNT.put(author,MORNING_COUNT);

        Double NOON_COUNT=this.NOON_COUNT.get(author);
        if(NOON_COUNT==null){
            NOON_COUNT=0.0;
        }
        NOON_COUNT=noon;
        this.NOON_COUNT.put(author,NOON_COUNT);

        Double AFTERNOON_COUNT=this.AFTERNOON_COUNT.get(author);
        if(AFTERNOON_COUNT==null){
            AFTERNOON_COUNT=0.0;
        }
        AFTERNOON_COUNT=afternoon;
        this.AFTERNOON_COUNT.put(author,AFTERNOON_COUNT);


        Double EVENING_COUNT=this.EVENING_COUNT.get(author);
        if(EVENING_COUNT==null){
            EVENING_COUNT=0.0;
        }
        EVENING_COUNT=evening;
        this.EVENING_COUNT.put(author,EVENING_COUNT);

        Double NIGHT_COUNT=this.NIGHT_COUNT.get(author);
        if(NIGHT_COUNT==null){
            NIGHT_COUNT=0.0;
        }
        NIGHT_COUNT=night;
        this.NIGHT_COUNT.put(author,NIGHT_COUNT);


        Double TS=this.TS.get(author);
        if(TS==null){
            TS=0.0;
        }

        Double SS=this.SS.get(author);
        if(SS==null){
            SS=0.0;
        }

        Double NON_CS=this.NON_CS.get(author);
        if(NON_CS==null){
            NON_CS=0.0;
        }

        Double RI=this.RI.get(author);
        if(RI==null){
            RI=0.0;
        }

        Double MI=this.MI.get(author);
        if(MI==null){
            MI=0.0;
        }

        Double ID=this.ID.get(author);
        if(ID==null){
            ID=0.0;
        }

        Double ID1=this.ID1.get(author);
        if(ID1==null){
            ID1=0.0;
        }

        Double NS=this.NS.get(author);
        if(NS==null){
            NS=0.0;
        }

        TS=(double)(OT1+CT1+RT1)/(double)numberOfTweets;
        this.TS.put(author,TS);


        if(OT1+RT1!=0){
            SS=OT1/(double)(OT1+RT1);
        }else {
            SS=1.0;
        }
        this.SS.put(author,SS);



        if(OT1+CT1!=0) {
            NON_CS = OT1 / (OT1 + CT1) + lamda * (CT1 - CT2) / (CT1 + 1);
        }else{
            NON_CS=lamda*(CT1-CT2)/(CT1+1);
        }
        this.NON_CS.put(author,NON_CS);


        RI=RT2*Math.log(RT3+1);
        this.RI.put(author,RI);


        MI=M3*Math.log(M4+1)-M1*Math.log(M2+1);
        this.MI.put(author,MI);


        ID1=Math.log((G3+1)/(G1+1))-Math.log((G4+1)/(G2+1));
        this.ID1.put(author,ID);

        ID=Math.log(G3+1)-Math.log(G4+1);
        this.ID.put(author,ID1);


        NS=Math.log(G1+1)-Math.log(G2+1);
        this.NS.put(author,NS);

        collector.emit(input,new Values(author,numberOfTweets,
                OT1,OT2,OT3,OT4,
                CT1,CT2,
                RT1,RT2,RT3,
                M1,M2,M3,M4,
                G1,G2,G3,G4,
                FREQUENCY,MORNING_COUNT,NOON_COUNT,AFTERNOON_COUNT,EVENING_COUNT,NIGHT_COUNT,
                TS,SS,NON_CS,RI,MI,ID,ID1,NS));




    }
}
