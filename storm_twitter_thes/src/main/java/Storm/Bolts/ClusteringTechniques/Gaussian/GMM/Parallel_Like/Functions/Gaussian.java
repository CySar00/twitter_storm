package Storm.Bolts.ClusteringTechniques.Gaussian.GMM.Parallel_Like.Functions;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/22/15.
 */
public class Gaussian {

    public static Map<String,double[]>gauss(Map<String,double[]>x,double[]mu,double[]sigma){
        Map<String,double[]>N=new HashMap<String, double[]>();

        for(String key:x.keySet()){
            double[] vector=x.get(key);
            double[]NVec=new double[vector.length];

            for(int i=0;i<vector.length;i++){
                NVec[i]=(1/Math.sqrt(sigma[i]*2*Math.PI))*(Math.exp(-(vector[i]-mu[i])*(vector[i]-mu[i])/2*sigma[i]));

                if(sigma[i]==0){
                    NVec[i]=(1/Math.sqrt(2*Math.PI))*(Math.exp(-(vector[i])*(vector[i])/2));
                }
            }
            N.put(key,NVec);
        }
        return N;
    }

    public static Map<String,double[]>gaussianPosteriorProbability(Map<String,double[]>x,double[]p){
        Map<String,double[]>posterior=new HashMap<String, double[]>();

        for(String key:x.keySet()){
            double[]vector=x.get(key);
            double[]gamma=new double[vector.length];
            double sum=0;

            for(int i=0;i<gamma.length;i++){
                gamma[i]=vector[i]*p[i];
            }

            for(int i=0;i<gamma.length;i++){
                sum+=gamma[i];
            }

            for(int i=0;i<gamma.length;i++){
                gamma[i]=gamma[i]/sum;
            }

            posterior.put(key,gamma);
        }
        return posterior;
    }

    public static double[]Nk(Map<String,double[]>gamma,int features){
        double[]n_k=new double[features];
        double[] vector=new double[gamma.keySet().size()];

        for(int i=0;i<features;i++){
            int count=0;double sum=0;

            for(String key:gamma.keySet()){
                vector[count]=gamma.get(key)[i];
                sum+=vector[count];
                count+=1;
            }
            n_k[i]=sum;
        }
        return n_k;
    }

    public static double[]pK(double[]Nk){
        double[]p=new double[Nk.length];

        for(int i=0; i<p.length;i++){
            p[i]=Nk[i]/ Nk.length;
        }
        return p;
    }

    public static double[]mu_k(double[]Nk,Map<String,double[]>gamma,Map<String,double[]>x){
        double[]mu=new double[Nk.length];

        for(int i=0;i<Nk.length;i++){
            double sum=0;

            for(String key:x.keySet()){
                sum+=x.get(key)[i]*gamma.get(key)[i];
            }
            mu[i]=sum/Nk[i];
        }
        return mu;
    }

    public static double[]sigma_k(double[]Nk,double[]mu,Map<String,double[]>gamma,Map<String,double[]>x){
        double []sigma=new double[Nk.length];

        for(int i=0;i<Nk.length;i++){
            double sum=0;

            for(String key:x.keySet()){
                sum+=gamma.get(key)[i]*(x.get(key)[i]-mu[i])*(x.get(key)[i]-mu[i]);
            }
            sigma[i]=sum/Nk[i];
        }
        return sigma;
    }

    public static Map<String,Double>logLikelihood(Map<String,double[]>x,double[]p){
        Map<String,Double>log=new HashMap<String, Double>();

        for(String key:x.keySet()){
            double sum=0;

            for(int i=0;i<x.get(key).length;i++){
                sum+=x.get(key)[i];
            }
            log.put(key,Math.log(sum));
        }
        return log;
    }

    public static double totalLogLikelihood(Map<String,Double>x){
        double sum=0;

        for(String key:x.keySet()){
            sum+=x.get(key);
        }
        return sum;
    }
}
