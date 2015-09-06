package Storm.Spouts.Twitter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by christina on 6/30/15.
 */
public class TweetFeedSpout extends BaseRichSpout {
    private static final String message="message";

    private final String accessToken;
    private final String accessTokenSecret;
    private final String consumerKey;
    private final String consumerSecret;

    private SpoutOutputCollector spoutOutputCollector;

    private TwitterStream twitterStream;
    private Twitter twitter;

    private LinkedBlockingQueue<Status> statuses;
    private FilterQuery filterQuery;
    private String[]filterWords;
    private String[]filterLanguages;

    public TweetFeedSpout(String accessToken,String accessTokenSecret,String consumerKey,String consumerSecret,String[]filterWords,String[] filterLanguages){
        if(accessToken==null || accessTokenSecret==null || consumerKey==null || consumerSecret==null){
            throw new RuntimeException("WFT fucking twitter4j OAuth Field cannot be fucking null");
        }
        this.accessToken=accessToken;
        this.accessTokenSecret=accessTokenSecret;
        this.consumerKey=consumerKey;
        this.consumerSecret=consumerSecret;


        this.filterWords=filterWords;
        this.filterLanguages=filterLanguages;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("STATUS","FOLLOWERS","FRIENDS "));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        statuses=new LinkedBlockingQueue<Status>(1000);
        this.spoutOutputCollector=collector;

        ConfigurationBuilder configurationBuilder=new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

        OAuthAuthorization authAuthorization=new OAuthAuthorization(configurationBuilder.build());


        twitterStream=new TwitterStreamFactory().getInstance(authAuthorization);
        twitterStream.addListener(new StatusListener() {
            @Override
            public void onStatus(Status status) {
                statuses.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {}

            @Override
            public void onStallWarning(StallWarning warning) {}

            @Override
            public void onException(Exception ex) {}
        });

        twitter=new TwitterFactory().getInstance(authAuthorization);
        filterQuery=new FilterQuery();

        if(filterQuery==null){
            twitterStream.sample();;
        }else{
            twitterStream.filter(filterQuery.track(filterWords));
            twitterStream.filter(filterQuery.language(filterLanguages));
        }


    }

    private boolean meetsConditions(Status status){
        return  true;
    }

    @Override
    public void nextTuple() {
        //emit tweets
        Status status=statuses.poll();
        if(status==null)
            Utils.sleep(1000);
        else{
            ResponseList<User>followers;
            ResponseList<User>friends;

            try {
                Thread.sleep(6000);
                followers=twitter.getFollowersList(status.getUser().getScreenName(),-1);
                Thread.sleep(6000);
                friends=twitter.getFriendsList(status.getUser().getScreenName(),-1);

                if(!followers.isEmpty() && !friends.isEmpty()) {
                    spoutOutputCollector.emit(new Values(status, followers, friends));
                }
                //followers.clear();
                //friends.clear();
            }catch (TwitterException ex){
                ex.printStackTrace();

            }catch (InterruptedException ex){
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
        super.close();
    }
}



