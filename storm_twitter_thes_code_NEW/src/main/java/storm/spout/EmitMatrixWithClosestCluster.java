package storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import storm.bolt.Clusters.databases.SerializerAndDeserializer;

import java.io.IOException;

/**
 * Created by christina on 6/19/15.
 */
public class EmitMatrixWithClosestCluster {

    public static int differenceBetweenMatrices(int [][]matrix1,int[][]matrix2){
        int diff=0;
        for(int i=0;i<matrix1.length;i++){
            for(int j=0;j<matrix1[i].length;j++){
                diff+=Math.abs(matrix1[i][j]-matrix2[i][j]);
            }
        }
        return diff;
    }

    public static void emitMatrixWithClosestClusterCenter(SpoutOutputCollector collector,String serializedMatrix[],int [][]matrix){
        int min=Integer.MAX_VALUE;int diff;int index=0;
        int[][]deserMatrix;
        int[][]returnMatrix=null;

        for(int i=0;i<serializedMatrix.length;i++){

            try{
                deserMatrix= SerializerAndDeserializer.deserialize(serializedMatrix[i]);
                diff=differenceBetweenMatrices(deserMatrix,matrix);

                if(diff<min){
                    min=diff;
                    index=i+1;
                    returnMatrix=deserMatrix;
                }
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        collector.emit(new Values(index,matrix));
    }
}
