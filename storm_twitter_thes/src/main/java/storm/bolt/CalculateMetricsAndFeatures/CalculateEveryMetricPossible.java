package storm.bolt.CalculateMetricsAndFeatures;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.sun.xml.internal.messaging.saaj.packaging.mime.internet.HeaderTokenizer;
import storm.bolt.CalculateMetricsAndFeatures.Functions.SimilarityScore;

import java.util.*;

/**
 * Created by christina on 3/28/15.
 */

public class CalculateEveryMetricPossible extends BaseRichBolt {
    private OutputCollector collector;

    Map<String,Double>OT1=new HashMap<String, Double>();
    Map<String,Double>OT2=new HashMap<String, Double>();

    Map<String,List<String>>userAndTexts=new HashMap<String, List<String>>();
    Map<String,Double>OT3=new HashMap<String, Double>();
    Map<String,Double>OT4=new HashMap<String, Double>();

    Map<String,Double>CT1=new HashMap<String, Double>();
    Map<String,Double>CT2=new HashMap<String, Double>();

    Map<String,Double>RT1=new HashMap<String, Double>();

    Map<String,List<Long>>userAndIDs=new HashMap<String, List<Long>>();
    Map<String,Double>RT2=new HashMap<String, Double>();
    Map<String,Double>RT3=new HashMap<String, Double>();

    Map<String,Double>M1=new HashMap<String, Double>();
    Map<String,Double>M2=new HashMap<String, Double>();
    Map<String,Double>M3=new HashMap<String, Double>();
    Map<String,Double>M4=new HashMap<String, Double>();

    Map<String,Double>G1=new HashMap<String, Double>();
    Map<String,Double>G2=new HashMap<String, Double>();
    Map<String,Double>G3=new HashMap<String, Double>();
    Map<String,Double>G4=new HashMap<String, Double>();
    Map<String,Date>userAndDate=new HashMap<String, Date>();

    Map<String,List<Date>>userAndListOfDates=new HashMap<String, List<Date>>();
    Map<String,Double>FREQUENCY=new HashMap<String, Double>();

    Map<String,Double>MORNING_COUNT=new HashMap<String, Double>();
    Map<String,Double>NOON_COUNT=new HashMap<String, Double>();
    Map<String,Double>AFTERNOON_COUNT=new HashMap<String, Double>();
    Map<String,Double>EVENING_COUNT=new HashMap<String, Double>();
    Map<String,Double>NIGHT_COUNT=new HashMap<String, Double>();


    Calendar calendar=Calendar.getInstance();

    String anAuthor;

    String author;
    Long ID;
    String text;
    Date date;
    Long inReplyToUserID;

    List<String>followers;
    List<String>friends;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","NUMBER_OF_TWEETS","OT1","OT2","OT3","OT4","CT1","CT2","RT1","RT2","RT3",
                                    "M1","M2","M3","M4","G1","G2","G3","G4","FREQUENCY","MORNING_COUNT","NOON_COUNT",
                                    "AFTERNOON_COUNT","EVENING_COUNT","NIGHT_COUNT"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        final String sourceComponent=input.getSourceComponent();
        String sourceStreamId=input.getSourceStreamId();

        if(sourceComponent.equals("CREATE_THE_AUTHORS")){
            anAuthor=input.getString(0);
        }

        if(sourceComponent.equals("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES")){
            author=input.getString(0);
            ID=input.getLong(1);
            text=input.getString(2);

            date=(Date)input.getValue(3);
             inReplyToUserID=input.getLong(7);
            followers=(List<String>)input.getValue(8);
            friends=(List<String>)input.getValue(9);

        }

        HashSet<String>authors=new HashSet<String>();

        if(anAuthor!=null && author!=null && text!=null){
            if(author.equals(anAuthor) ) {
                if (!authors.contains(author)) {

                    final List<String> hashtags = new ArrayList<String>();
                    final List<String> URLs = new ArrayList<String>();
                    final List<String> atUsers = new ArrayList<String>();


                    final List<String> uniqueUsersThatRTdTheAuthorsTweet = new ArrayList<String>();

                    final List<String> atUniqueUsers = new ArrayList<String>();
                    final List<String> mentionsOfTheAuthor = new ArrayList<String>();
                    final List<String> uniqueOtherUsersThatMentionTheAuthor = new ArrayList<String>();

                    final List<String> topicallyActiveFollowers = new ArrayList<String>();
                    final List<String> topicallyActiveFriends = new ArrayList<String>();


                    Double OT1 = this.OT1.get(author);
                    if (OT1 == null)
                        OT1 = 0.0;

                    Double OT2 = this.OT2.get(author);
                    if (OT2 == null)
                        OT2 = 0.0;

                    List<String> texts = this.userAndTexts.get(author);
                    if (texts == null)
                        texts = new ArrayList<String>();
                    texts.add(text);

                    Double OT3 = this.OT3.get(author);
                    if (OT3 == null)
                        OT3 = 0.0;

                    Double OT4 = this.OT4.get(author);
                    if (OT4 == null)
                        OT4 = 0.0;

                    Double CT1 = this.CT1.get(author);
                    if (CT1 == null)
                        CT1 = 0.0;

                    Double CT2 = this.CT2.get(author);
                    if (CT2 == null)
                        CT2 = 0.0;

                    Double RT1 = this.RT1.get(author);
                    if (RT1 == null)
                        RT1 = 0.0;

                    List<Long> IDs = userAndIDs.get(author);
                    if (IDs == null)
                        IDs = new ArrayList<Long>();
                    IDs.add(ID);

                    Double RT2 = this.RT2.get(author);
                    if (RT2 == null)
                        RT2 = 0.0;

                    Double RT3 = this.RT3.get(author);
                    if (RT3 == null)
                        RT3 = 0.0;

                    Double M1 = this.M1.get(author);
                    if (M1 == null)
                        M1 = 0.0;

                    Double M2 = this.M2.get(author);
                    if (M2 == null)
                        M2 = 0.0;

                    Double M3 = this.M3.get(author);
                    if (M3 == null)
                        M3 = 0.0;

                    Double M4 = this.M4.get(author);
                    if (M4 == null)
                        M4 = 0.0;

                    Double G1 = this.G1.get(author);
                    if (G1 == null)
                        G1 = 0.0;

                    Double G2 = this.G2.get(author);
                    if (G2 == null)
                        G2 = 0.0;

                    Double G3 = this.G3.get(author);
                    if (G3 == null)
                        G3 = 0.0;

                    Double G4 = this.G4.get(author);
                    if (G4 == null)
                        G4 = 0.0;

                    List<Date> dates = userAndListOfDates.get(author);
                    if (dates == null)
                        dates = new ArrayList<Date>();
                    dates.add(date);

                    Double FREQUENCY = this.FREQUENCY.get(author);
                    if (FREQUENCY == null)
                        FREQUENCY = 0.0;

                    Double MORNING_COUNT = this.MORNING_COUNT.get(author);
                    if (MORNING_COUNT == null)
                        MORNING_COUNT = 0.0;

                    Double NOON_COUNT = this.NOON_COUNT.get(author);
                    if (NOON_COUNT == null)
                        NOON_COUNT = 0.0;

                    Double AFTERNOON_COUNT = this.AFTERNOON_COUNT.get(author);
                    if (AFTERNOON_COUNT == null)
                        AFTERNOON_COUNT = 0.0;

                    Double EVENING_COUNT = this.EVENING_COUNT.get(author);
                    if (EVENING_COUNT == null)
                        EVENING_COUNT = 0.0;

                    Double NIGHT_COUNT = this.NIGHT_COUNT.get(author);
                    if (NIGHT_COUNT == null)
                        NIGHT_COUNT = 0.0;


                    StringTokenizer stringTokenizer = new StringTokenizer(text);
                    while (stringTokenizer.hasMoreTokens()) {
                        String token = stringTokenizer.nextToken();

                    if (token.startsWith("#")) {
                        hashtags.add(token);
                    }

                    if (token.startsWith("http:")) {
                        URLs.add(token);
                    }

                    if (token.startsWith("@")) {
                        atUsers.add(token);
                        if (!atUniqueUsers.contains(token)) {
                            atUniqueUsers.add(token);
                        }
                    }
                }

                if ((!text.startsWith("@") && !text.contains("RT")) || !text.startsWith("RT")) {
                    OT1 += 1;
                }

                OT2 += URLs.size();

                double tempLevenshtein = 0;
                for (int i = 0; i < texts.size(); i++) {
                    String text_i = texts.get(i);

                    for (int j = 0; j < texts.size(); j++) {
                        String text_j = texts.get(j);

                        tempLevenshtein = SimilarityScore.computeLevenshteinDistance(text_i, text_j);
                    }
                }

                if (texts.size() == 1) {
                    OT3 = tempLevenshtein;
                } else {
                    OT3 = 2 * tempLevenshtein / (texts.size() * (texts.size() - 1));
                }

                OT4 += hashtags.size();

                if (text.startsWith("@")) {
                    CT1 += 1;
                }

                if (text.startsWith("@") && inReplyToUserID == -1) {
                    CT2 += 1;
                }

                if (text.startsWith("RT") || text.contains("RT")) {
                    RT1 += 1;
                }


                if ((!text.contains("RT") && !text.startsWith("@")) || (!text.startsWith("RT"))) {
                    int lastIndex = 0;
                    while (lastIndex != -1) {
                        lastIndex = this.userAndIDs.entrySet().toString().indexOf(Long.toString(ID), lastIndex + Long.toString(ID).length());
                        if (lastIndex == -1) {
                            RT2 += 1;
                        }
                    }
                }
                M1 += atUsers.size();

                M2 = Double.valueOf(atUniqueUsers.size());

                Iterator<Map.Entry<String, List<String>>> entryIterator = this.userAndTexts.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<String, List<String>> entry = entryIterator.next();

                    String username = entry.getKey();
                    List<String> textsPerEntry = entry.getValue();

                    Iterator<String> iterator = textsPerEntry.iterator();
                    while (iterator.hasNext()) {
                        String aText = iterator.next();

                        if (aText.startsWith("RT") || aText.contains("RT")) {
                            StringTokenizer stringTokenizer1 = new StringTokenizer(aText);
                            while (stringTokenizer1.hasMoreTokens()) {
                                String aToken = stringTokenizer1.nextToken();
                                if (aToken.startsWith("@")) {
                                    String tempToken = aToken.replace("@", "");
                                    if (tempToken.equals(author)) {
                                        if (!uniqueUsersThatRTdTheAuthorsTweet.contains(username)) {
                                            uniqueUsersThatRTdTheAuthorsTweet.add(username);
                                        }
                                    }
                                }
                            }
                        }

                        StringTokenizer stringTokenizer1 = new StringTokenizer(aText);
                        while (stringTokenizer1.hasMoreTokens()) {
                            String aToken = stringTokenizer1.nextToken();
                            if (aToken.startsWith("@")) {
                                String tempToken = aToken.replace("@", "");
                                if (tempToken.equals(author)) {
                                    M3 += 1;
                                    if (!uniqueOtherUsersThatMentionTheAuthor.contains(username)) {
                                        uniqueOtherUsersThatMentionTheAuthor.add(username);
                                    }
                                }
                            }
                        }
                    }

                    if (followers.contains(username)) {
                        if (!topicallyActiveFollowers.contains(username)) {
                            topicallyActiveFollowers.add(username);
                        }
                    }

                    if (friends.contains(username)) {
                        if (!topicallyActiveFriends.contains(username)) {
                            topicallyActiveFriends.add(username);
                        }
                    }
                }

                M4 = Double.valueOf(uniqueOtherUsersThatMentionTheAuthor.size());

                G1 = Double.valueOf(topicallyActiveFollowers.size());
                G2 = Double.valueOf(topicallyActiveFriends.size());

                Iterator<Map.Entry<String, List<Date>>> entryIterator1 = userAndListOfDates.entrySet().iterator();
                while (entryIterator1.hasNext()) {
                    Map.Entry<String, List<Date>> entry = entryIterator1.next();

                    String key = entry.getKey();
                    List<Date> listOfDates = entry.getValue();

                    if (topicallyActiveFollowers.contains(key)) {
                        for (Date aDate : listOfDates) {
                            if (aDate.after(date)) {
                                G3 += 1;
                            }
                        }
                    }

                    if (topicallyActiveFriends.contains(key)) {
                        for (Date aDate : listOfDates) {
                            if (aDate.before(date)) {
                                G4 += 1;
                            }
                        }
                    }
                }

                Date firstDate = dates.get(0);
                Date lastDate = dates.get(dates.size() - 1);

                double duration = (lastDate.getTime() - firstDate.getTime()) / 60 * 1000;
                if (duration == 0) {
                    FREQUENCY = 0.0;
                } else {
                    FREQUENCY = texts.size() / duration;
                }

                if (calendar.get(Calendar.HOUR_OF_DAY) >= 5 && calendar.get(Calendar.HOUR_OF_DAY) < 12) {
                    MORNING_COUNT += 1;
                }

                if (calendar.get(Calendar.HOUR_OF_DAY) == 12) {
                    NOON_COUNT += 1;
                }

                if (calendar.get(Calendar.HOUR_OF_DAY) > 12 && calendar.get(Calendar.HOUR_OF_DAY) <= 16) {
                    AFTERNOON_COUNT += 1;
                }

                if (calendar.get(Calendar.HOUR_OF_DAY) > 16 && calendar.get(Calendar.HOUR_OF_DAY) <= 21) {
                    EVENING_COUNT += 1;
                }

                if ((calendar.get(Calendar.HOUR_OF_DAY) > 21 && calendar.get(Calendar.HOUR_OF_DAY) < 24) || (calendar.get(Calendar.HOUR_OF_DAY) >= 0 && calendar.get(Calendar.HOUR_OF_DAY) < 5)) {
                    NIGHT_COUNT += 1;
                }

//                    if(!authors.contains(author)){
                if (!this.OT1.containsKey(author) && !this.OT2.containsKey(author) && !this.OT3.containsKey(author) && !this.OT4.containsKey(author) && !this.CT1.containsKey(author) && !this.CT2.containsKey(author) && !this.RT1.containsKey(author) && !this.RT2.containsKey(author) && !this.RT3.containsKey(author) && !this.M1.containsKey(author) && !this.M2.containsKey(author) && !this.M3.containsKey(author) && !this.M4.containsKey(author) && !this.G1.containsKey(author) && !this.G2.containsKey(author) && !this.G3.containsKey(author) && !this.G4.containsKey(author) && !this.FREQUENCY.containsKey(author) && !this.MORNING_COUNT.containsKey(author) && !this.NOON_COUNT.containsKey(author) && !this.AFTERNOON_COUNT.containsKey(author) && !this.NIGHT_COUNT.containsKey(author) && !this.userAndTexts.containsKey(author) && !this.userAndIDs.containsKey(author) && !this.userAndListOfDates.containsKey(author)) {
                        System.out.println(author);

                        collector.emit(new Values(author, texts.size(), OT1, OT2, OT3, OT4, CT1, CT2, RT1, RT2, RT3, M1, M2, M3, M4, G1, G2, G3, G4, FREQUENCY, MORNING_COUNT, NOON_COUNT, AFTERNOON_COUNT, EVENING_COUNT, NIGHT_COUNT));

                        this.OT1.put(author, OT1);
                        this.OT2.put(author, OT2);
                        this.userAndTexts.put(author, texts);
                        this.OT3.put(author, OT3);
                        this.OT4.put(author, OT4);

                        this.CT1.put(author, CT1);
                        this.CT2.put(author, CT2);

                        this.RT1.put(author, RT1);
                        this.userAndIDs.put(author, IDs);
                        this.RT2.put(author, RT2);
                        this.RT3.put(author, RT3);


                        this.M1.put(author, M1);
                        this.M2.put(author, M2);
                        this.M3.put(author, M3);
                        this.M4.put(author, M4);

                        this.G1.put(author, G1);
                        this.G2.put(author, G2);
                        this.G3.put(author, G3);
                        this.G4.put(author, G4);

                        this.userAndListOfDates.put(author, dates);
                        this.FREQUENCY.put(author, FREQUENCY);
                        this.MORNING_COUNT.put(author, MORNING_COUNT);
                        this.NOON_COUNT.put(author, NOON_COUNT);
                        this.AFTERNOON_COUNT.put(author, AFTERNOON_COUNT);
                        this.EVENING_COUNT.put(author, EVENING_COUNT);
                        this.NIGHT_COUNT.put(author, NIGHT_COUNT);

                        authors.add(author);
                    }
                }
            }
        }
        collector.ack(input);
    }
}

