package storm.bolt.DataProcessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 6/27/15.
 */
public class MapUsersFromDataSet extends BaseBasicBolt {
    private OutputCollector collector;

    List<String>theAuthors=new ArrayList<String>();
    Map<String,String>theAuthorAndTweets=new HashMap<String, String>();
    Map<String,Long>authorAndTweetID=new HashMap<String, Long>();

    Map<String,List<String>>authorAndHashTags=new HashMap<String, List<String>>();
    Map<String,List<String>>authorAndURLs=new HashMap<String, List<String>>();
    Map<String,List<String>>authorAndUsers=new HashMap<String, List<String>>();

    Map<String,List<String>>authorAndFollowers=new HashMap<String, List<String>>();
    Map<String,List<String>>authorAndFriends=new HashMap<String, List<String>>();

    Map<Long,Date>idAndDate=new HashMap<Long,Date>();
    Map<Long,Long>idAndInReplyToUserId=new HashMap<Long, Long>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("USERNAME","TWEET","#TAGS","URLS","@USERS","FOLLOWERS","FRIENDS","CREATED_AT","IN_REPLY","TWEET_ID"));

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        final String sourceComponent=input.getSourceComponent();
        String author;

        if(sourceComponent.equals("CREATE_THE_AUTHORS")){
             author=input.getString(0);

            if(author!=null){
                if(!theAuthors.contains(author)) {
                    theAuthors.add(author);
                    System.out.println(author);
                }
            }
        }

        if(sourceComponent.equals("SPLIT_LINES_INTO_TUPLE_FIELD_VALUES")){
            author=input.getString(0);
            Long tweetID=input.getLong(1);
            String zeTweet=input.getString(2);

            Date createdAt=(Date)input.getValue(3);
            List<String>hashtags=(List<String>)input.getValue(4);
            List<String>urls=(List<String>)input.getValue(5);
            List<String>userMentions=(List<String>)input.getValue(6);
            Long inReplyToTweet=input.getLong(7);
            List<String>zeFollowers=(List<String >)input.getValue(8);
            List<String>zeFriends=(List<String>)input.getValue(9);



            if(author!=null && zeTweet!=null && createdAt!=null && hashtags!=null && urls!=null && userMentions!=null && zeFollowers!=null && zeFriends!=null){
                System.out.println(author+" "+zeTweet);

                theAuthorAndTweets.put(author,zeTweet);

                authorAndHashTags.put(author,hashtags);
                authorAndURLs.put(author,urls);
                authorAndUsers.put(author,userMentions);

                authorAndFollowers.put(author,zeFollowers);
                authorAndFriends.put(author,zeFriends);

                authorAndTweetID.put(author,tweetID);
                idAndDate.put(tweetID,createdAt);
                idAndInReplyToUserId.put(tweetID,inReplyToTweet);

            }
        }

        if(theAuthors!=null || !theAuthors.isEmpty()) {
            if (theAuthorAndTweets != null || !theAuthorAndTweets.isEmpty()) {

                searchListAndMap(theAuthors, theAuthorAndTweets, authorAndHashTags, authorAndURLs, authorAndUsers, authorAndFollowers, authorAndFriends, authorAndTweetID, idAndDate, idAndInReplyToUserId, collector);
            }
        }
    }

    private void searchListAndMap(List<String>list,Map<String,String>map,Map<String,List<String>>map1,Map<String,List<String>>map2,
                                  Map<String,List<String>>map3,Map<String ,List<String>>map4,Map<String,List<String>>map5,Map<String,Long>map6,
                                  Map<Long,Date>map7,Map<Long,Long>map8,
                                  BasicOutputCollector collector1){

        if(list!=null || !list.isEmpty()){
            if(map!=null || !map.isEmpty()){

                for(String stringInList:list) {
                    if(stringInList!=null) {
                        if (map.containsKey(stringInList) && map1.containsKey(stringInList) && map2.containsKey(stringInList) && map3.containsKey(stringInList) && map4.containsKey(stringInList) && map5.containsKey(stringInList) && map6.containsKey(stringInList)) {
                            String author = stringInList;

                            String tweet = map.get(stringInList);
                            List<String> hashtags = map1.get(stringInList);
                            List<String> urls = map2.get(stringInList);
                            List<String> atUsers = map3.get(stringInList);
                            List<String> followers = map4.get(stringInList);
                            List<String> friends = map5.get(stringInList);

                            Long tweetID = map6.get(stringInList);

                            Date date = map7.get(tweetID);
                            Long inReplyTo = map8.get(tweetID);

                            collector1.emit(new Values(stringInList,tweet,hashtags,urls,atUsers,followers,friends,date,inReplyTo,tweetID));
                            }
                        }
                    }
                }
            }
        }
}
