package storm.bolt.Clusters;

/**
 * Created by christina on 6/19/15.
 */
public class MatrixOperations {

    public static int[][]addMatrices(int [][]matrix1,int [][]matrix2,int valueOfN){
        int [][]matrix3=new int[matrix1.length][matrix1[0].length];
        for(int i=0;i<matrix1.length;i++){
            for(int j=0;j<matrix1[i].length;j++){
                matrix3[i][j]=(valueOfN*matrix2[i][j]+matrix1[i][j])/(valueOfN+1);
            }
        }
        return matrix3;
    }
}
