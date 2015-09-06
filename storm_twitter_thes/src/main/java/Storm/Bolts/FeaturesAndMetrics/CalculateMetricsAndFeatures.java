package Storm.Bolts.FeaturesAndMetrics;

import Storm.Bolts.FeaturesAndMetrics.Functions.SimilarityScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 7/16/15.
 */
public class CalculateMetricsAndFeatures extends BaseRichBolt {
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

    Map<String,Double>TS=new HashMap<String, Double>();
    Map<String,Double>SS=new HashMap<String, Double>();
    Map<String,Double>NON_CS=new HashMap<String, Double>();
    Map<String ,Double>RI=new HashMap<String, Double>();
    Map<String ,Double>MI=new HashMap<String, Double>();
    Map<String,Double>ID=new HashMap<String, Double>();
    Map<String ,Double>ID1=new HashMap<String, Double>();
    Map<String ,Double>NS=new HashMap<String, Double>();

    Calendar calendar;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","NUMBER_OF_TWEETS",
                                    "OT1","OT2","OT3","OT4",
                                    "CT1","CT2",
                                    "RT1","RT2","RT3",
                                    "M1","M2","M3","M4",
                                    "G1","G2","G3","G4",
                                    "FREQUENCY","MORNING_COUNT","NOON_COUNT","AFTERNOON_COUNT","EVENING_COUNT","NIGHT_COUNT",
                                    "TS","SS","NON_CS","RI","MI","ID","ID1","NS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        tweets=new HashMap<String, List<String>>();
        IDs=new HashMap<String, List<Long>>();
        users=new HashMap<String, List<String>>();
        dates=new HashMap<String, List<Date>>();
        followers=new HashMap<String, List<String>>();
        friends=new HashMap<String, List<String>>();

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

        calendar=Calendar.getInstance();
    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        Long id=input.getLong(1);
        String tweet=input.getString(2);
        Date date=(Date)input.getValue(3);

        Long inReplyTo=input.getLong(4);
        List<String>followers1=(List<String>)input.getValue(5);
        List<String>friends1=(List<String>)input.getValue(6);

        List<String>tweets=this.tweets.get(author);
        if(tweets==null){
            tweets=new ArrayList<String>();
        }
        tweets.add(tweet);
        this.tweets.put(author,tweets);

        List<Long>IDs=this.IDs.get(author);
        if(IDs==null){
            IDs=new ArrayList<Long>();
        }
        IDs.add(id);
        this.IDs.put(author,IDs);

        List<Date>dates=this.dates.get(author);
        if(dates==null){
            dates=new ArrayList<Date>();
        }
        dates.add(date);
        this.dates.put(author,dates);

        List<String>users=this.users.get(author);
        if(users==null){
            users=new ArrayList<String>();
        }
        for(String user:getMentionedUsersInATweet(tweet)){
            users.add(user);
        }
        this.users.put(author,users);

        List<String>followers=this.followers.get(author);
        if(followers==null){
            followers=new ArrayList<String>();
        }
        for(String follower1:followers1){
            followers.add(follower1);
        }
        this.followers.put(author,followers);

        List<String>friends=this.friends.get(author);
        if(friends==null){
            friends=new ArrayList<String>();
        }
        for(String friend1:friends1){
            friends.add(friend1);
        }
        this.friends.put(author,friends);

        Double numberOfTweets=this.numberOfTweets.get(author);
        if(numberOfTweets==null){
            numberOfTweets=0.0;
        }

        Double OT1=this.OT1.get(author);
        if(OT1==null){
            OT1=0.0;
        }

        Double OT2=this.OT2.get(author);
        if(OT2==null){
            OT2=0.0;
        }

        Double OT3=this.OT3.get(author);
        if(OT3==null){
            OT3=0.0;
        }

        Double OT4=this.OT4.get(author);
        if(OT4==null){
            OT4=0.0;
        }

        Double CT1=this.CT1.get(author);
        if(CT1==null){
            CT1=0.0;
        }

        Double CT2=this.CT2.get(author);
        if(CT2==null){
            CT2=0.0;
        }

        Double RT1=this.RT1.get(author);
        if(RT1==null){
            RT1=0.0;
        }

        Double RT2=this.RT2.get(author);
        if(RT2==null){
            RT2=0.0;
        }

        Double RT3=this.RT3.get(author);
        if(RT3==null){
            RT3=0.0;
        }

        Double M1=this.M1.get(author);
        if(M1==null){
            M1=0.0;
        }

        Double M2=this.M2.get(author);
        if(M2==null){
            M2=0.0;
        }

        Double M3=this.M3.get(author);
        if(M3==null){
            M3=0.0;
        }

        Double M4=this.M4.get(author);
        if(M4==null){
            M4=0.0;
        }

        Double G1=this.G1.get(author);
        if(G1==null){
            G1=0.0;
        }

        Double G2=this.G2.get(author);
        if(G2==null){
            G2=0.0;
        }

        Double G3=this.G3.get(author);
        if(G3==null){
            G3=0.0;
        }

        Double G4=this.G4.get(author);
        if(G4==null){
            G4=0.0;
        }

        Double FREQUENCY=this.FREQUENCY.get(author);
        if(FREQUENCY==null){
            FREQUENCY=0.0;
        }

        Double MORNING_COUNT=this.MORNING_COUNT.get(author);
        if(MORNING_COUNT==null){
            MORNING_COUNT=0.0;
        }

        Double NOON_COUNT=this.NOON_COUNT.get(author);
        if(NOON_COUNT==null){
            NOON_COUNT=0.0;
        }

        Double AFTERNOON_COUNT=this.AFTERNOON_COUNT.get(author);
        if(AFTERNOON_COUNT==null){
            AFTERNOON_COUNT=0.0;
        }

        Double EVENING_COUNT=this.EVENING_COUNT.get(author);
        if(EVENING_COUNT==null){
            EVENING_COUNT=0.0;
        }

        Double NIGHT_COUNT=this.NIGHT_COUNT.get(author);
        if(NIGHT_COUNT==null){
            NIGHT_COUNT=0.0;
        }


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

        numberOfTweets+=1;
        this.numberOfTweets.put(author,numberOfTweets);

        if(!tweet.startsWith("@") && !tweet.contains("RT") && !tweet.startsWith("RT")){
            OT1+=1;
        }
        this.OT1.put(author,OT1);

        OT2+=countURLsInATweet(tweet);
        this.OT2.put(author,OT2);

        double leveneshtein=0;
        for(int i=0;i<tweets.size();i++){
            String tweet_i=tweets.get(i);

            for(int j=i+1;j<tweets.size();j++){
                String tweet_j=tweets.get(j);

                leveneshtein+= SimilarityScore.computeLevenshteinDistance(tweet_i,tweet_j);
            }
        }
        if(tweets.size()==1){
            OT3=leveneshtein;
        }else{
            OT3=2*leveneshtein/(tweets.size()*(tweets.size()-1));
        }
        this.OT3.put(author,OT3);

        OT4+=countHashtagsInATweet(tweet);
        this.OT4.put(author,OT4);


        if(tweet.startsWith("@")){
            CT1+=1;
        }
        this.CT1.put(author,CT1);


        if(tweet.startsWith("@")){
            if(inReplyTo==-1){
                CT2+=1;
            }
        }
        this.CT2.put(author,CT2);


        if(tweet.startsWith("RT") || tweet.contains("RT")){
            RT1+=1;
        }
        this.RT1.put(author,RT1);

        int lastIndex=0;
        if(tweet.startsWith("RT") || tweet.contains("RT")) {
            while (lastIndex != -1) {
                lastIndex=this.IDs.entrySet().toString().indexOf(Long.toString(id),lastIndex+Long.toString(id).length());
                if (lastIndex == -1) {
                    RT2 += 1;
                }
            }
        }
        this.RT2.put(author,RT2);

        RT3=countUniqueUsersThatRTdTheAuthorsTweets(this.tweets,author);
        this.RT3.put(author,RT3);

        M1+=countMentionedUsersInATweet(tweet);
        this.M1.put(author,M1);

        M2+=countUniqueMentionedUsersInATweet(tweet);
        this.M2.put(author,M2);

        M3=countMentionsOfTheAuthorInOtherPeoplesTweets(author,this.tweets);
        this.M3.put(author,M3);

        M4=countUniqueUsersThatMentionTheAuthorInATweet(author,this.tweets);
        this.M4.put(author,M4);

        G1=countTopicallyActiveFollowers(followers,this.tweets);
        this.G1.put(author,G1);

        G2=countTopicallyActiveFriends(friends,this.tweets);
        this.G2.put(author,G2);

        G3+=countFollowersThatPostedBeforTheAuthor(followers,this.dates,date);
        this.G3.put(author,G3);

        G4+=countFriendsThatPostedAfterTheAuthor(friends,this.dates,date);
        this.G4.put(author, G4);



        long duration=(dates.get(dates.size()-1).getTime()-dates.get(0).getTime())/60*1000;
        if(duration!=0){
            FREQUENCY=numberOfTweets/duration;
        }

        calendar.setTime(date);
        if(calendar.get(Calendar.HOUR_OF_DAY)>=5 && calendar.get(Calendar.HOUR_OF_DAY)<12){
            MORNING_COUNT+=1;
        }
        if (calendar.get(Calendar.HOUR_OF_DAY)==12){
            NOON_COUNT+=1;
        }

        if(calendar.get(Calendar.HOUR_OF_DAY)>12 && calendar.get(Calendar.HOUR_OF_DAY)<=17){
            AFTERNOON_COUNT+=1;
        }

        if(calendar.get(Calendar.HOUR_OF_DAY)>17 && calendar.get(Calendar.HOUR_OF_DAY)<=21){
            EVENING_COUNT+=1;
        }

        if((calendar.get(Calendar.HOUR_OF_DAY)>21 && calendar.get(Calendar.HOUR_OF_DAY)<24) || (calendar.get(Calendar.HOUR_OF_DAY)>=0 && calendar.get(Calendar.HOUR_OF_DAY)<5)){
            NIGHT_COUNT+=1;
        }

        this.FREQUENCY.put(author,FREQUENCY);

        this.MORNING_COUNT.put(author,MORNING_COUNT);
        this.NOON_COUNT.put(author,NOON_COUNT);
        this.AFTERNOON_COUNT.put(author,AFTERNOON_COUNT);
        this.EVENING_COUNT.put(author,EVENING_COUNT);
        this.NIGHT_COUNT.put(author,NIGHT_COUNT);


        TS=(OT1+CT1+RT1)/numberOfTweets;


        if(OT1+RT1!=0) {
            SS = OT1 / (OT1 + RT1);
        }else{
            SS=1.0;
        }

        if(OT1+CT1!=0) {
            NON_CS = OT1 / (OT1 + CT1) + lamda * (CT1 - CT2) / (CT1 + 1);
        }else{
            NON_CS=lamda*(CT1-CT2)/(CT1+1);
        }

        RI=RT2*Math.log(RT3+1);

        MI=M3*Math.log(M4+1)-M1*Math.log(M2+1);

        ID1=Math.log((G3+1)/(G1+1))-Math.log((G4+1)/(G2+1));

        ID=Math.log(G3+1)-Math.log(G4+1);

        NS=Math.log(G1+1)-Math.log(G2+1);


        System.out.println(author+" "+OT1+" "+OT2+" "+" "+OT3+" "+OT4);


        this.TS.put(author,TS);
        this.SS.put(author,SS);
        this.NON_CS.put(author,NON_CS);
        this.RI.put(author,RI);
        this.MI.put(author,MI);
        this.ID.put(author,ID);
        this.ID1.put(author,ID1);
        this.NS.put(author,NS);

        collector.emit(input,new Values(author,numberOfTweets,
                                        OT1,OT2,OT3,OT4,
                                        CT1,CT2,
                                        RT1,RT2,RT3,
                                        M1,M2,M3,M4,
                                        G1,G2,G3,G4,
                                        FREQUENCY,MORNING_COUNT,NOON_COUNT,AFTERNOON_COUNT,EVENING_COUNT,NIGHT_COUNT,
                                        TS,SS,NON_CS,RI,MI,ID,ID1,NS));


      //  collector.ack(input);

    }

    private Double countHashtagsInATweet(String tweet){
        List<String>hashtags=new ArrayList<String>();
        String[]words=tweet.split(" ");
        for(String word:words){
            if(word.startsWith("#") || word.contains("#")){
                hashtags.add(word);
            }
        }
        return (double)hashtags.size();
    }

    private Double countURLsInATweet(String tweet){
        List<String>URLs=new ArrayList<String>();
        String[]words=tweet.split(" ");
        for(String word:words){
            if(word.startsWith("http") || word.contains("http")){
                URLs.add(word);
            }
        }
        return (double)URLs.size();
    }

    private Double countUniqueUsersThatRTdTheAuthorsTweets(Map<String,List<String>>tweets,String author){
        List<String>otherUsers=new ArrayList<String>();
        Set<String>set=new HashSet<String>();

        for(Map.Entry<String,List<String>>twEntry:tweets.entrySet()){
            String author1=twEntry.getKey();
            List<String>tweets1=twEntry.getValue();

            for(String tweet1:tweets1){
                if(tweet1.startsWith("RT") ||tweet1.contains("RT")){
                    String[]words=tweet1.split(" ");
                    for(String word:words){
                        if(word.startsWith("@")){
                            String word1=word.replace("@","");
                            if(word1.equals(author)){
                                if(!set.contains(author1)){
                                    otherUsers.add(author1);
                                    set.add(author1);
                                }
                            }
                        }
                    }
                }
            }
        }
        return  (double)otherUsers.size();
    }


    private List<String>getMentionedUsersInATweet(String tweet){
        List<String>mentionedUsers=new ArrayList<String>();
        String[]words=tweet.split(" ");
        for(String word:words){
            if(word.startsWith("@")){
                String word1=word.replace("@","");
                mentionedUsers.add(word1);
            }
        }
        return mentionedUsers;
    }

    private Double countMentionedUsersInATweet(String tweet){
        List<String>mentionedUsers=new ArrayList<String>();
        String[]words=tweet.split(" ");
        for(String word:words){
            if(word.startsWith("@")){
                String word1=word.replace("@","");
                mentionedUsers.add(word1);
            }
        }
        return (double)mentionedUsers.size();
    }

    private Double countUniqueMentionedUsersInATweet(String tweet){
        List<String>mentionedUsers=new ArrayList<String>();
        Set<String>set=new HashSet<String>();
        String[]words=tweet.split(" ");
        for(String word:words){
            if(word.startsWith("@")){
                if(!set.contains(word)){
                    mentionedUsers.add(word);
                    set.add(word);
                }
            }
        }
        return (double)mentionedUsers.size();
    }
    private Double countMentionsOfTheAuthorInOtherPeoplesTweets(String author,Map<String,List<String>>tweets){
        List<String>authors=new ArrayList<String>();
        for(Map.Entry<String,List<String>>twEntry:tweets.entrySet()){
            List<String>tweets1=twEntry.getValue();
            for(String tweet1:tweets1){
                String[]words=tweet1.split(" ");
                for(String word:words){
                    if(word.startsWith("@")){
                        String word1=word.replace("@","");
                        if(word1.equals(author)){
                            authors.add(author);
                        }
                    }
                }
            }
        }
        return (double)authors.size();
    }

    private Double countUniqueUsersThatMentionTheAuthorInATweet(String author,Map<String,List<String>>tweets){
        List<String>users=new ArrayList<String>();
        Set<String>set=new HashSet<String>();

        for(Map.Entry<String,List<String>>twEntry:tweets.entrySet()){
            String author1=twEntry.getKey();
            List<String>tweets1=twEntry.getValue();
            for(String tweet1:tweets1){
                String[]words=tweet1.split(" ");
                for(String word:words){
                    if(word.startsWith("@")){
                        String word1=word.replace("@","");
                        if(word1.equals(author)){
                            if(!set.contains(author1)){
                                users.add(author1);
                                set.add(author1);
                            }
                        }
                    }
                }
            }
        }
        return (double)users.size();
    }




    private Double countTopicallyActiveFollowers(List<String>followers,Map<String,List<String>>tweets){
        List<String>activeFollowers=new ArrayList<String>();

        for(String follower:followers){
            if(tweets.containsKey(follower)){
                activeFollowers.add(follower);
            }
        }
        return (double)activeFollowers.size();
    }

    private Double countTopicallyActiveFriends(List<String>friends,Map<String,List<String>>tweets){
        List<String>activeFriends=new ArrayList<String>();

        for(String follower:friends){
            if(tweets.containsKey(follower)){
                activeFriends.add(follower);
            }
        }
        return (double)activeFriends.size();
    }





    private Double countFollowersThatPostedBeforTheAuthor(List<String>followers,Map<String ,List<Date>>dates,Date date){
        List<String>followers1=new ArrayList<String>();
        for(String follower:followers){
            if(dates.containsKey(follower)){
                for(Date date1:dates.get(follower)){
                    if(date1.after(date)){
                        followers1.add(follower);
                    }
                }
            }
        }
        return  (double)followers1.size();
    }

    private Double countFriendsThatPostedAfterTheAuthor(List<String>friends,Map<String,List<Date>>dates,Date date){
        List<String>friends1=new ArrayList<String>();
        for(String friend:friends){
            if(dates.containsKey(friend)){
                for(Date date1:dates.get(friend)){
                    if(date1.before(date)){
                        friends1.add(friend);
                    }
                }
            }
        }
        return  (double)friends1.size();

    }

}
