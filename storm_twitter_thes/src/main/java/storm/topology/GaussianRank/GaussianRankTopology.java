package storm.topology.GaussianRank;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.FileWriterBolt;
import storm.bolt.GaussianRankAndMixtureModel.DataMaps.ReadDataFromMap;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.CalculateGaussianDistribution;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.CalculateTheInitialMeansAndCovarianceForEachFeature;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.CreateTheMixtureModelDataMap;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.EStep.CalculateGaussianPosteriorProbability;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheCovariance;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheGaussianDistribution;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheMeans;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheMixtureModelCoefficients;
import storm.bolt.GaussianRankAndMixtureModel.Rank.ComputeGaussianCDF;
import storm.bolt.GaussianRankAndMixtureModel.Rank.CreateMapsFromMixtureModelMapsFile;
import storm.bolt.GaussianRankAndMixtureModel.Rank.Rank;
import storm.spout.ReadLastLineSpout;

/**
 * Created by christina on 4/3/15.
 */
public class GaussianRankTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MIXTURE_MODEL_PARAMETER_MAP_1_FROM_FILE",new ReadLastLineSpout("MixtureModelMapsWithCluster1Data.txt"));
        topologyBuilder.setBolt("CREATE_MIXTURE_MODEL_MAPS_1",new CreateMapsFromMixtureModelMapsFile()).shuffleGrouping("READ_MIXTURE_MODEL_PARAMETER_MAP_1_FROM_FILE");

        topologyBuilder.setBolt("COMPUTE_GAUSSIAN_CDF",new ComputeGaussianCDF()).globalGrouping("CREATE_MIXTURE_MODEL_MAPS_1");
        topologyBuilder.setBolt("RANK",new Rank()).globalGrouping("COMPUTE_GAUSSIAN_CDF");



        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Test",config,topologyBuilder.createTopology());
            Utils.sleep(900000);
            localCluster.killTopology("Test");
            localCluster.shutdown();
        }
    }

}
