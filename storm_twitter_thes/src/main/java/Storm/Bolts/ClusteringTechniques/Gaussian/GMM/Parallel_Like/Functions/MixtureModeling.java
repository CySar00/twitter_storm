package Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Parallel_Like.Functions;

import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import sun.print.BackgroundServiceLookup;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/22/15.
 */
public class MixtureModeling {

    public static void MixtureModeling(Map<String,double[]> map,OutputCollector collector,int features){
        Map<String,double[]>gamma=new HashMap<String, double[]>();
        double[]Nk=new double[features];
        double[]mu_k=new double[features];
        double[]sigma_k=new double[features];
        double[]pK=new double[features];

        double[]mu=BasicStatistics.calculateMUForAVector(map,features);
        System.out.println(Doubles.asList(mu));

        double[]sigma=BasicStatistics.calculateTheSigmaForAVector(map,features);
        System.out.println(Doubles.asList(sigma));

        double[]p=BasicStatistics.probability(features);
        System.out.println(Doubles.asList(p));

        //the initial gaussian mixture model:
        Map<String,double[]>N =Gaussian.gauss(map,mu,sigma);

        int iterations=0;
        //the initial log likelihood::
        double logLikelihood1=Gaussian.totalLogLikelihood(Gaussian.logLikelihood(map,p));
        double logLikelihood2=Double.MIN_VALUE;

      //  gamma=Gaussian.gaussianPosteriorProbability(map,p);

        //Nk=Gaussian.Nk(gamma,features);
        //pK=Gaussian.pK(Nk);
        //mu_k=Gaussian.mu_k(Nk,gamma,map);
        //sigma_k=Gaussian.sigma_k(Nk,mu,gamma,map);


        do {
            //expectation step::
            System.out.println("LOG_i"+logLikelihood1);
            logLikelihood1=logLikelihood2;
            gamma=Gaussian.gaussianPosteriorProbability(map,p);

            //maximization step::
            Nk=Gaussian.Nk(gamma,features);
            pK=Gaussian.pK(Nk);
            mu_k=Gaussian.mu_k(Nk,gamma,map);
            sigma_k=Gaussian.sigma_k(Nk,mu,gamma,map);

            mu=mu_k;
            sigma=sigma_k;
            p=pK;

            System.out.println("MU "+Doubles.asList(mu));
            System.out.println("SIGMA "+Doubles.asList(sigma));
            System.out.println("P "+Doubles.asList(p));

            logLikelihood2=Gaussian.totalLogLikelihood(Gaussian.logLikelihood(Gaussian.gauss(map,mu,sigma),p));
            System.out.println("LOG_i+1 "+logLikelihood2);

        }while (Math.abs(logLikelihood2-logLikelihood1)/Math.abs(logLikelihood1)>=0.0001);

        Storm.Databases.CassandraDB.EM.StatisticsValuesDB.setSerializedStatisticsValues(0,Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.serializeJavaDoublesList(Doubles.asList(mu)));
        Storm.Databases.CassandraDB.EM.StatisticsValuesDB.setSerializedStatisticsValues(1,Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.serializeJavaDoublesList(Doubles.asList(sigma)));
        Storm.Databases.CassandraDB.EM.StatisticsValuesDB.setSerializedStatisticsValues(2,Storm.Databases.Functions.SerializeAndDeserializeJavaObjects.serializeJavaDoublesList(Doubles.asList(p)));
        
        System.out.println("FINAL MU :"+Doubles.asList(mu));
        System.out.println("FINAL SIGMA: "+Doubles.asList(sigma));

        collector.emit(new Values(map));



    }
}
