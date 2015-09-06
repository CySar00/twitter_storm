package Storm.Bolts.FeaturesAndMetrics.Metrics;

import Storm.Bolts.FeaturesAndMetrics.Functions.SimilarityScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import sun.misc.BASE64Decoder;

import javax.print.attribute.standard.MediaSize;
import java.util.*;

/**
 * Created by christina on 7/25/15.
 */
public class Metrics extends BaseRichBolt {
    OutputCollector collector;

    Map<String,Integer>numberOfTweets;

    Map<String,Integer>OT1;
    Map<String,Integer>OT2;
    Map<String,Double>OT3;
    Map<String,Integer>OT4;

    Map<String,Integer>CT1;
    Map<String,Integer>CT2;

    Map<String,Integer>RT1;
    Map<String,Integer>RT2;
    Map<String,Integer>RT3;

    Map<String,Integer>M1;
    Map<String,Integer>M2;
    Map<String,Integer>M3;
    Map<String,Integer>M4;

    Map<String,Integer>G1;
    Map<String,Integer>G2;
    Map<String,Integer>G3;
    Map<String,Integer>G4;

    Map<String,Double>FREQUENCY;
    Map<String,Integer>MORNING;
    Map<String,Integer>NOON;
    Map<String,Integer>AFTERNOON;
    Map<String,Integer>EVENING;
    Map<String,Integer>NIGHT;

    Calendar calendar;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","NUMBER_OF_TWEETS",
                                    "OT1","OT2","OT3","OT4",
                                    "CT1","CT2",
                                    "RT1","RT2","RT3",
                                    "M1","M2","M3","M4",
                                    "G1","G2","G3","G4",
                                    "FREQUENCY","MORNING","NOON","AFTERNOON","EVENING","NIGHT"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        numberOfTweets=new HashMap<String, Integer>();
        OT1=new HashMap<String, Integer>();
        OT2=new HashMap<String, Integer>();
        OT3=new HashMap<String, Double>();
        OT4=new HashMap<String, Integer>();

        CT1=new HashMap<String, Integer>();
        CT2=new HashMap<String, Integer>();

        RT1=new HashMap<String, Integer>();
        RT2=new HashMap<String, Integer>();
        RT3=new HashMap<String, Integer>();

        M1=new HashMap<String, Integer>();
        M2=new HashMap<String, Integer>();
        M3=new HashMap<String, Integer>();
        M4=new HashMap<String, Integer>();

        G1=new HashMap<String, Integer>();
        G2=new HashMap<String, Integer>();
        G3=new HashMap<String, Integer>();
        G4=new HashMap<String, Integer>();

        FREQUENCY=new HashMap<String, Double>();

        MORNING=new HashMap<String, Integer>();
        NOON=new HashMap<String, Integer>();
        AFTERNOON=new HashMap<String, Integer>();
        EVENING=new HashMap<String, Integer>();
        NIGHT=new HashMap<String, Integer>();

        calendar=Calendar.getInstance();

    }

    @Override
    public void execute(Tuple input) {
        String author = input.getString(0);

        List<Long>IDs=(List<Long>)input.getValue(1);
        List<String>tweets=(List<String>)input.getValue(2);
        List<Date>dates=(List<Date>)input.getValue(3);
        List<Long>inReplyTos=(List<Long>)input.getValue(4);

        List<String>followers=(List<String>)input.getValue(5);
        List<String>friends=(List<String>)input.getValue(6);

        Map<String,List<Double>>allIDs=(Map<String,List<Double>>)input.getValue(7);
        Map<String,List<String>>allTweets=(Map<String,List<String>>)input.getValue(8);
        Map<String,List<Date>>allDates=(Map<String,List<Date>>)input.getValue(9);


        Integer numberOfTweets=this.numberOfTweets.get(author);
        if(numberOfTweets==null){
            numberOfTweets=0;
        }
        numberOfTweets=tweets.size();
        this.numberOfTweets.put(author,numberOfTweets);

        Integer OT1=this.OT1.get(author);
        if(OT1==null){
            OT1=0;
        }

        Integer OT2=this.OT2.get(author);
        if(OT2==null){
            OT2=0;
        }

        Double OT3=this.OT3.get(author);
        if(OT3==null){
            OT3=0.0;
        }

        Integer OT4=this.OT4.get(author);
        if(OT4==null){
            OT4=0;
        }

        Integer CT1=this.CT1.get(author);
        if(CT1==null){
            CT1=0;
        }

        Integer CT2=this.CT2.get(author);
        if(CT2==null){
            CT2=0;
        }

        Integer RT1=this.RT1.get(author);
        if(RT1==null){
            RT1=0;
        }

        Integer RT2=this.RT2.get(author);
        if(RT2==null){
            RT2=0;
        }

        Integer RT3=this.RT3.get(author);
        if(RT3==null){
            RT3=0;
        }

        Integer M1=this.M1.get(author);
        if(M1==null){
            M1=0;
        }

        Integer M2=this.M2.get(author);
        if(M2==null){
            M2=0;
        }

        Integer M3=this.M3.get(author);
        if(M3==null){
            M3=0;
        }

        Integer M4=this.M4.get(author);
        if(M4==null){
            M4=0;
        }

        Integer G1=this.G1.get(author);
        if(G1==null){
            G1=0;
        }

        Integer G2=this.G2.get(author);
        if(G2==null){
            G2=0;
        }

        Integer G3=this.G3.get(author);
        if(G3==null){
            G3=0;
        }

        Integer G4=this.G4.get(author);
        if(G4==null){
            G4=0;
        }

        Double FREQUENCY=this.FREQUENCY.get(author);
        if(FREQUENCY==null){
            FREQUENCY=0.0;
        }

        Integer MORNING=this.MORNING.get(author);
        if(MORNING==null){
            MORNING=0;
        }

        Integer NOON=this.NOON.get(author);
        if(NOON==null){
            NOON=0;
        }

        Integer AFTERNOON=this.AFTERNOON.get(author);
        if(AFTERNOON==null){
            AFTERNOON=0;
        }

        Integer EVENING=this.EVENING.get(author);
        if(EVENING==null){
            EVENING=0;
        }

        Integer NIGHT=this.NIGHT.get(author);
        if(NIGHT==null){
            NIGHT=0;
        }

        HashSet<String>unique1=new HashSet<String>();
        HashSet<String>unique2=new HashSet<String>();

        for(String tweet:tweets){

            if(!tweet.startsWith("@") && !tweet.contains("RT")){
                OT1+=1;
            }

            if(tweet.startsWith("@")){
                CT1+=1;
            }

            if(tweet.contains("RT")){
                RT1+=1;
            }

            String[]words=tweet.split(" ");

            for(String word:words){

                if(word.contains("http")){
                    OT2+=1;
                }

                if(word.startsWith("#")){
                    OT4+=1;
                }

                if(word.startsWith("@")){
                    M1+=1;

                    if(!unique1.contains(word)){
                        unique1.add(word);
                    }
                }
            }
        }
        M2=unique1.size();

        this.OT1.put(author,OT1);
        this.OT2.put(author,OT2);
        this.OT4.put(author,OT4);
        this.CT1.put(author,CT1);
        this.RT1.put(author,RT1);
        this.M1.put(author,M1);
        this.M2.put(author,M2);

        double levenshtein=0;
        for(int i=0;i<tweets.size();i++){
            for(int j=i+1;j<tweets.size();j++){

                levenshtein+=SimilarityScore.computeLevenshteinDistance(tweets.get(i),tweets.get(j));
            }
        }

        if(tweets.size()==1){
            OT3=levenshtein;
        }else{
            OT3=2*levenshtein/(tweets.size()*(tweets.size()-1));
        }
        this.OT3.put(author,OT3);

        for(int i=0;i<tweets.size();i++){
            for(int j=0;j<inReplyTos.size();j++){

                if(i==j){
                    if(tweets.get(i).startsWith("@") && inReplyTos.get(j)==-1){
                        CT2+=1;
                    }
                }
            }
        }
        this.CT2.put(author,CT2);

        for(int i=0;i<tweets.size();i++){
            for(int j=0;j<IDs.size();j++){

                if(i==j){
                    if(tweets.get(i).contains("RT")){
                        int lastIndex=-1;

                        while (lastIndex!=-1){
                            lastIndex=allIDs.entrySet().toString().indexOf(Long.toString(IDs.get(j),lastIndex+Long.toString(IDs.get(j)).length()));
                            if(lastIndex==-1){
                                RT2+=1;
                            }
                        }
                    }
                }
            }
        }
        this.RT2.put(author,RT2);

        Set<String>unique3=new HashSet<String>();

        for(Map.Entry<String,List<String>>entry1:allTweets.entrySet()){
            String randomUser=entry1.getKey();
            List<String>tweetsOfRandomUser=entry1.getValue();

            for(String tweet:tweetsOfRandomUser){

                if(tweet.contains("RT")) {
                    String[] words = tweet.split(" ");
                    for (String word : words) {
                        if (word.startsWith("@")) {
                            String word1 = word.replace("@", "");
                            if (word1.equals(author)) {
                                if (unique2.contains(word1)) {
                                    unique2.add(word1);
                                }
                            }
                        }
                    }
                }

                String[]words=tweet.split(" ");

                for(String word:words){
                    if(word.startsWith("@")){
                        String word1=word.replace("@","");
                        if(word1.equals(author)){
                            M3+=1;

                            if(!unique3.contains(randomUser)){
                                unique3.add(randomUser);
                            }
                        }
                    }
                }
            }
        }
        RT3=unique2.size();
        M4=unique3.size();
        this.RT3.put(author,RT3);
        this.M3.put(author,M3);
        this.M4.put(author,M4);

        for(String follower:followers){
           for(Map.Entry<String,List<Date>>entry:allDates.entrySet()){
               String randomAuthor=entry.getKey();
               List<Date>datesOfRandomAuthor=entry.getValue();

               if(follower.equals(randomAuthor)){
                   G1+=1;

                   for(Date randomDate:datesOfRandomAuthor){
                       for(Date date:dates){
                           if(randomDate.after(date)){
                               G3+=1;
                           }
                       }
                    }
               }
           }
        }
        this.G1.put(author,G1);
        this.G3.put(author,G3);

        for(String friend:friends){
            for(Map.Entry<String,List<Date>>entry:allDates.entrySet()){
                String randomAuthor=entry.getKey();
                List<Date>datesOfRandomAuthor=entry.getValue();

                if(friend.equals(randomAuthor)){
                    G2+=1;

                    for(Date randomDate:datesOfRandomAuthor){
                        for(Date date:dates){
                            if(randomDate.before(date)){
                                G4+=1;
                            }
                        }
                    }
                }
            }
        }
        this.G2.put(author,G2);
        this.G3.put(author,G4);

        Date last=dates.get(dates.size()-1);
        Date first=dates.get(0);

        double duration=(last.getTime()-first.getTime())/60*1000;

        FREQUENCY=dates.size()/duration;
        if(duration==0){
            FREQUENCY=1.0;
        }
        this.FREQUENCY.put(author,FREQUENCY);

        for(Date date:dates){
            calendar.setTime(date);

            if(calendar.get(Calendar.HOUR_OF_DAY)>=5 && calendar.get(Calendar.HOUR_OF_DAY)<12){
                MORNING+=1;
            }

            if(calendar.get(Calendar.HOUR_OF_DAY)==12){
                NOON+=1;
            }

            if(calendar.get(Calendar.HOUR_OF_DAY)>12 && calendar.get(Calendar.HOUR_OF_DAY)<17){
                AFTERNOON+=1;
            }


            if(calendar.get(Calendar.HOUR_OF_DAY)>=17 && calendar.get(Calendar.HOUR_OF_DAY)<21){
                EVENING+=1;
            }

            if((calendar.get(Calendar.HOUR_OF_DAY)>=21 && calendar.get(Calendar.HOUR_OF_DAY)<24) || (calendar.get(Calendar.HOUR_OF_DAY)>=0 && calendar.get(Calendar.HOUR_OF_DAY)<5)){
                NIGHT+=1;
            }
        }
        this.MORNING.put(author,MORNING);
        this.NOON.put(author,NOON);
        this.AFTERNOON.put(author,AFTERNOON);
        this.EVENING.put(author,EVENING);
        this.NIGHT.put(author,NIGHT);

        collector.emit(input,new Values(author,numberOfTweets,OT1,OT2,OT3,OT4,
                                        CT1,CT2,
                                        RT1,RT2,RT3,
                                        M1,M2,M3,M4,
                                        G1,G2,G3,G4,
                                        FREQUENCY,MORNING,NOON,AFTERNOON,EVENING,NIGHT));

    }
}
