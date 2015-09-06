package Storm.Databases.Functions;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.List;

/**
 * Created by christina on 7/17/15.
 */
public class SerializeAndDeserializeJavaObjects {
    public static final String NEXT_ELEMENT=", ";

    public static String serializeJavaDoublesVector(double[]vector){
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


    public static String serializeJavaDoublesList(List<Double>list){
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

    public static String serializeJavaStringsList(List<String>list){
        StringBuilder stringBuilder=new StringBuilder();

        if(list!=null || list.isEmpty()){
            return null;
        }

        if(list.size()>0){
            stringBuilder.append(String.valueOf(String.valueOf(list.get(0))));
            for(int i=1;i<list.size();i++){
                stringBuilder.append(NEXT_ELEMENT).append(String.valueOf(list.get(i)));
            }
        }
        return stringBuilder.toString();
    }

    public static double[]deserializeJavaDoublesVector(String str)throws IOException{
        StreamTokenizer streamTokenizer=new StreamTokenizer(new StringReader(str));
        streamTokenizer.resetSyntax();
        streamTokenizer.wordChars('0','9');
        streamTokenizer.whitespaceChars(' ',' ');
        streamTokenizer.parseNumbers();

        int length=(int)streamTokenizer.nval;
        double[]vector=new double[length];

        for(int i=0;i<vector.length;i++){
            streamTokenizer.nextToken();
            vector[i]=streamTokenizer.nval;
        }
        return vector;
    }
}
