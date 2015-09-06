package storm.topology.GaussianRank;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


import storm.bolt.FileWriterBolt;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.CalculateGaussianDistribution;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.CalculateTheInitialMeansAndCovarianceForEachFeature;
import storm.bolt.GaussianRankAndMixtureModel.DataMaps.ReadDataFromMap;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.CreateTheMixtureModelDataMap;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.EStep.CalculateGaussianPosteriorProbability;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheCovariance;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheGaussianDistribution;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheMeans;
import storm.bolt.GaussianRankAndMixtureModel.MixtureModel.EMAlgorithm.MStep.ReCalculateTheMixtureModelCoefficients;
import storm.spout.ReadLastLineSpout;

/**
 * Created by christina on 3/28/15.
 */
public class GaussianMixtureModelTopology {
    public static void main(String[]args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("READ_MAP_WITH_DATA_FROM_K_CLUSTER_EQUALS_1_FROM_FILE",new ReadLastLineSpout("WriteKClusterMapWithIndex1ToFile.txt"));
        topologyBuilder.setBolt("CREATE_DATA_MAP_FROM_K_CLUSTER_EQUALS_1_FILE",new ReadDataFromMap()).globalGrouping("READ_MAP_WITH_DATA_FROM_K_CLUSTER_EQUALS_1_FROM_FILE");

        topologyBuilder.setBolt("CALCULATE_INITIAL_MIXTURE_MODEL_PARAMETERS_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new CalculateTheInitialMeansAndCovarianceForEachFeature()).globalGrouping("CREATE_DATA_MAP_FROM_K_CLUSTER_EQUALS_1_FILE");
        topologyBuilder.setBolt("CALCULATE_THE_GAUSSIAN_DISTRIBUTION_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new CalculateGaussianDistribution()).fieldsGrouping("CALCULATE_INITIAL_MIXTURE_MODEL_PARAMETERS_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new Fields("AUTHOR"));
        //E-Step Of The EM Algorithm
        topologyBuilder.setBolt("CALCULATE_THE_GAUSSIAN_POSTERIOR_PROBABILITY_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new CalculateGaussianPosteriorProbability()).fieldsGrouping("CALCULATE_THE_GAUSSIAN_DISTRIBUTION_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new Fields("AUTHOR"));
        //M-Step Of The EM Algorithm
        topologyBuilder.setBolt("RECALCULATE_THE_GAUSSIAN_DISTRIBUTION_AKA_N_K_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new ReCalculateTheGaussianDistribution()).fieldsGrouping("CALCULATE_THE_GAUSSIAN_POSTERIOR_PROBABILITY_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new Fields("AUTHOR"));
        topologyBuilder.setBolt("RECALCULATE_THE_MEANS_AKA_MEANS_K_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new ReCalculateTheMeans()).fieldsGrouping("RECALCULATE_THE_GAUSSIAN_DISTRIBUTION_AKA_N_K_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new Fields("AUTHOR"));
        topologyBuilder.setBolt("RECALCULATE_THE_COVARIANCE_AKA_SIGMA_K_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new ReCalculateTheCovariance()).fieldsGrouping("RECALCULATE_THE_MEANS_AKA_MEANS_K_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new Fields("AUTHOR"));
        topologyBuilder.setBolt("RECALCULATE_THE_MIXTURE_COEFFICIENTS_AKA_PK_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new ReCalculateTheMixtureModelCoefficients()).fieldsGrouping("RECALCULATE_THE_COVARIANCE_AKA_SIGMA_K_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new Fields("AUTHOR"));

        topologyBuilder.setBolt("EMIT_ALL_RECALCULATED_MIXTURE_MODEL_PARAMETERS_INTO_MAP_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA",new CreateTheMixtureModelDataMap()).shuffleGrouping("RECALCULATE_THE_MIXTURE_COEFFICIENTS_AKA_PK_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA");
        topologyBuilder.setBolt("WRITE_MIXTURE_MODEL_MAP_INTO_TEXT_FILE",new FileWriterBolt("MixtureModelMapsWithCluster1Data.txt")).shuffleGrouping("EMIT_ALL_RECALCULATED_MIXTURE_MODEL_PARAMETERS_INTO_MAP_FOR_EACH_FEATURE_OF_THE_K_CLUSTER_EQUALS_1_DATA");


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
