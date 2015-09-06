package Storm.Topologies.TweetFeed.PerSubject;

import Storm.Bolts.Databases.MongoDB.WriteMarvelTweetsToMongoDB;
import Storm.Spouts.Twitter.TweetFeedSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import twitter4j.FilterQuery;

/**
 * Created by christina on 9/2/15.
 */
public class TwitterTopology_Elections2015b {
    private static String consumerKey = "KccnMTJrSxl2g9c51cWh9qXPl";
    private static String consumerSecret = "3qtqsbmL16aQZt6efMPoNfxfNBRTBk5kU4T79VFsvyGPimsrYm";
    private static String accessToken = "99579271-4QI6OY8CDpm1YkK6nrJYW1ZD5tl5dNHUjf4Emwcf3";
    private static String accessTokenSecret = "mCHLBdyCRwFtlLEukVHbkuYBQ1Xx9Hq60cmrn26cJAjyA";

    private static final String[]queryWords={"ekloges","ekloges2015","εκλογές","ekloges","ekloges_2015","alphaekloges"};
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

        TweetFeedSpout twitterSpout=new TweetFeedSpout(accessToken,accessTokenSecret,consumerKey,consumerSecret,queryWords,languages);
        builder.setSpout("TWITTER_SPOUT",twitterSpout,1);

        builder.setBolt("WRITE_TWEET_FEEDS_TO_MONGODB",new WriteMarvelTweetsToMongoDB()).globalGrouping("TWITTER_SPOUT");

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
