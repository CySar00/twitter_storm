package Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank;

import Storm.Bolts.ClusteringTechniques.Gaussian.GaussianRank.Functions.CDFs;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/22/15.
 */
public class CalculatingTheCDFs extends BaseRichBolt {
    OutputCollector collector;

    Map<String,List<Double>>list;
    Map<String,double[]>vector;
    Map<String,double[]>CDF;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("AUTHOR","CDFS","CDFS_AS_VECTORS"));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

        list=new HashMap<String, List<Double>>();
        vector=new HashMap<String, double[]>();
        CDF=new HashMap<String, double[]>();

    }

    @Override
    public void execute(Tuple input) {
        String author=input.getString(0);
        List<Double>listOfValues=(List<Double>)input.getValue(1);
        double[]vector1= Doubles.toArray(listOfValues);

        List<Double>list=this.list.get(author);
        if(list==null){
            list=new ArrayList<Double>();
        }
        for(int i=0;i<listOfValues.size();i++){
            list.add(listOfValues.get(i));
        }
        this.list.put(author,list);

        double[]vector=this.vector.get(author);
        if(vector==null){
            vector=new double[vector1.length];
        }
        vector=vector1;

        String []serial=Storm.Databases.CassandraDB.EM.StatisticsValuesDB.getSerializedStatisticsValues();
        String meansStr=serial[0];
        String sigmaStr=serial[1];
        List<Double>meansL=new ArrayList<Double>();
        List<Double>sigmaL=new ArrayList<Double>();

        String[]splitMeans=meansStr.split(",");
        String[]splitSigma=sigmaStr.split(",");

        for(int i=0;i<splitMeans.length;i++){
            meansL.add(Double.valueOf(splitMeans[i]));
            sigmaL.add(Double.valueOf(splitSigma[i]));
        }
        double[]mu=Doubles.toArray(meansL);
        double[]sigma=Doubles.toArray(sigmaL);

        double[]cdf=new double[vector.length];
        double[]z=new double[vector.length];
        for(int i=0;i<cdf.length;i++){

            if(sigma[i]!=0){
                z[i]=(vector[i]-mu[i])/sigma[i];
            }else{
                z[i]=vector[i];
            }
            cdf[i]=CDFs.Phi(z[i]);
        }

        double[]CDF=this.CDF.get(author);
        if(CDF==null){
            CDF=new double[cdf.length];
        }
        CDF=cdf;
        this.CDF.put(author,CDF);



        collector.emit(input,new Values(author,Doubles.asList(cdf),cdf));
      //  System.out.println("THE CDFS:");
        //System.out.println(author+" "+Doubles.asList(cdf));

    }
}
