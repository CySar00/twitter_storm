package storm.topology.DataAndUsers.TweetFeedTopologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.bolt.DataProcessing.TweetProcessBolt;
import storm.bolt.Databases.MongoDB.WriteTweetFeedsToAMongoDB1;
import storm.bolt.Databases.MongoDB.WriteTweetFeedsToMongoDB;
import storm.spout.TwitterFeedSpout;
import twitter4j.FilterQuery;

/**
 * Created by christina on 6/19/15.
 */
public class TweetFeedTopoloy_NEW {
    private static String consumerKey = "KccnMTJrSxl2g9c51cWh9qXPl";
    private static String consumerSecret = "3qtqsbmL16aQZt6efMPoNfxfNBRTBk5kU4T79VFsvyGPimsrYm";
    private static String accessToken = "99579271-4QI6OY8CDpm1YkK6nrJYW1ZD5tl5dNHUjf4Emwcf3";
    private static String accessTokenSecret = "mCHLBdyCRwFtlLEukVHbkuYBQ1Xx9Hq60cmrn26cJAjyA";

    private static final String[]queryWords={"Bloomberg","Μένουμε Ευρώπη","#nationalkissingday","#EMFYLIOS","caitlyn","iAmCaitlyn","MenoumeEvropi","braduatika","grexit","GOT","game of thrones","GoodForYou","LoveWins","eurogroup","GoodBandNameBadFirstDate"};
    private static  final String []languages={"en","el"};


    public static void main(String[] args) throws Exception{
        /**         SETUP       **/
        String remoteClusterToplogyName=null;
        if(args!=null){
            if(args.length==1){
                remoteClusterToplogyName=args[0];
            }else if(args.length==4){
                //if credentials are provided as commandline arguments
                accessToken=args[0];
                accessTokenSecret=args[1];
                consumerKey=args[2];
                consumerSecret=args[3];
            }
        }

        TopologyBuilder builder=new TopologyBuilder();

        FilterQuery filterQuery=new FilterQuery();
        filterQuery.language(languages);



        TwitterFeedSpout twitterSpout=new TwitterFeedSpout(accessToken,accessTokenSecret,consumerKey,consumerSecret,queryWords,languages);
        builder.setSpout("TWITTER_SPOUT",twitterSpout,2);

       builder.setBolt("WRITE_TWEET_FEEDS_TO_MONGODB",new WriteTweetFeedsToAMongoDB1()).globalGrouping("TWITTER_SPOUT");



        /***********************************/

        /**     SETUP CONFIG     **/

        Config config=new Config();
        config.setDebug(false);

        if(remoteClusterToplogyName!=null){
            config.setNumWorkers(8);
            config.setMaxSpoutPending(1000);
            config.setNumAckers(4);
            config.setMessageTimeoutSecs(30);
            StormSubmitter.submitTopology(remoteClusterToplogyName, config, builder.createTopology());

        }else{
            config.setMaxTaskParallelism(20);
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("TwitterTopology",config,builder.createTopology());
            Thread.sleep(60 * 1000 * 100000000);
            cluster.killTopology("TwitterTopology");

            cluster.shutdown();
//            System.exit(0);
        }
    }
}
