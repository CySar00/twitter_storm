package storm.bolt.Clustering.Functions;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by christina on 4/1/15.
 */
public class SerializeAndDeserializeJavaObjects {
    public static final char NEXT_ELEMENT=',';

    public static String convertDoubleVectorToString(double[]vector){
        StringBuilder stringBuilder=new StringBuilder();

        if(vector==null){
            return null;
        }

        if(vector.length>0){
            stringBuilder.append(String.valueOf(vector[0]));
            for(int i=1;i<vector.length;i++){
                stringBuilder.append(NEXT_ELEMENT).append(String.valueOf(vector[i]));
            }
        }
        return stringBuilder.toString();
    }

    public static String convertDoubleListToString(List<Double>list){
        StringBuilder stringBuilder=new StringBuilder();

        if(list==null || list.isEmpty()){
            return null;
        }

        if(list.size()>0){
            stringBuilder.append(String.valueOf(list.get(0)));
            for(int i=1;i<list.size();i++){
                stringBuilder.append(NEXT_ELEMENT).append(String.valueOf(list.get(i)));
            }
        }
        return stringBuilder.toString();
    }


    public static double[] convertStringToDoubleArray(String string) throws IOException{
        StreamTokenizer streamTokenizer=new StreamTokenizer(new StringReader(string));
        streamTokenizer.resetSyntax();
        streamTokenizer.wordChars('0','9');
        streamTokenizer.whitespaceChars(' ',' ');
        streamTokenizer.parseNumbers();

       // streamTokenizer.nextToken();
        int length=(int)streamTokenizer.nval;
        double[]vector=new double[length];
        for(int i=0;i<vector.length;i++){
            streamTokenizer.nextToken();
            vector[i]=streamTokenizer.nval;
        }
        return vector;
    }
}
