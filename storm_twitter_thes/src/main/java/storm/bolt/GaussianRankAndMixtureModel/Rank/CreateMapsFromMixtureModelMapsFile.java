package storm.bolt.GaussianRankAndMixtureModel.Rank;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;
import org.apache.commons.lang.StringUtils;
import org.mortbay.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 4/3/15.
 */
public class CreateMapsFromMixtureModelMapsFile extends BaseRichBolt {
    OutputCollector collector;

    Map<String,double[]>features=new HashMap<String, double[]>();
    Map<String,double[]>means=new HashMap<String, double[]>();
    Map<String,double[]>sigma=new HashMap<String, double[]>();

    Map<String,String>tempFeatures=new HashMap<String, String>();
    Map<String,String>tempMeans=new HashMap<String, String>();
    Map<String,String>tempSigma=new HashMap<String, String>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("FEATURES","MEANS","SIGMA"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String lastLineFromLine=input.getString(0);

        int firstIndexOfMaps=lastLineFromLine.indexOf("[{");
        int lastIndexOfMap=lastLineFromLine.indexOf("]}]");

        String fuckingMapsString=lastLineFromLine.substring(firstIndexOfMaps+2,lastIndexOfMap+1);
        String[]mapsString=fuckingMapsString.split("\\}, \\{");

        String featuresMapString=mapsString[0];
        String meansMap=mapsString[1];
        String sigmaMap=mapsString[2];

        String[] authorAndFeatures=featuresMapString.split("\\]");


        for(int i=0;i<authorAndFeatures.length;i++){
            String theAuthor= StringUtils.substringBefore(authorAndFeatures[i], "=");
            String author=theAuthor.replace(",","");
            String listOfFeatures=StringUtils.substringAfter(authorAndFeatures[i], "=[");

         //   System.out.println(author+listOfFeatures);
            tempFeatures.put(author,listOfFeatures);
        }

        for(Map.Entry<String,String>entry:tempFeatures.entrySet()){
            String theAuthor=entry.getKey();
            String theFeatures=entry.getValue();

            String[]features=theFeatures.split(",");
            List<Double> listOfFeatures=new ArrayList<Double>();
            for(int i=0;i<features.length;i++){
                listOfFeatures.add(Double.parseDouble(features[i]));
            }
            double[]featuresVector= Doubles.toArray(listOfFeatures);
            this.features.put(theAuthor,featuresVector);
        }

        String[]authorAndMeans=meansMap.split("\\]");
        for(int i=0;i<authorAndMeans.length;i++){
            String theAuthor=StringUtils.substringBefore(authorAndMeans[i],"=");
            String author=theAuthor.replace(",","");
            String listOfMeans=StringUtils.substringAfter(authorAndMeans[i],"=[");
            tempMeans.put(author,listOfMeans);
        }

        for(Map.Entry<String,String>entry:tempMeans.entrySet()){
            String theAuthor=entry.getKey();
            String theMeans=entry.getValue();

            String[]means=theMeans.split(",");
            List<Double>listOfMeans=new ArrayList<Double>();
            for(int i=0;i<means.length;i++){
                listOfMeans.add(Double.parseDouble(means[i]));
            }
            double[]meansVector=Doubles.toArray(listOfMeans);
            this.means.put(theAuthor,meansVector);
        }

        String[] authorAndSigma=sigmaMap.split("\\]");
        for(int i=0;i<authorAndSigma.length;i++){
            String theAuthor=StringUtils.substringBefore(authorAndSigma[i], "=");
            String author=theAuthor.replace(",","");
            String theSigmas=StringUtils.substringAfter(authorAndSigma[i],"=[");

            //System.out.println(author+" "+theSigmas);
            tempSigma.put(author,theSigmas);
        }

        for(Map.Entry<String,String>entry:tempSigma.entrySet()){
            String theAuthor=entry.getKey();
            String theSigmas=entry.getValue();

            String[]sigma=theSigmas.split(",");
            List<Double>listOfSigmas=new ArrayList<Double>();
            for(int i=0;i<sigma.length;i++){
                listOfSigmas.add(Double.parseDouble(sigma[i]));
            }
            double[]sigmaVector=Doubles.toArray(listOfSigmas);
            this.sigma.put(theAuthor,sigmaVector);
        }

        collector.emit(new Values(features,means,sigma));
    }
}
