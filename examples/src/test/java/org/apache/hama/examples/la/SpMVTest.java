package org.apache.hama.examples.la;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hama.HamaConfiguration;
import org.junit.Before;
import org.junit.Test;

public class SpMVTest {
  private HamaConfiguration conf;
  private FileSystem fs;
  private String baseDir;
  
  @Before
  public void prepare() throws IOException {
    conf = new HamaConfiguration();
    fs = FileSystem.get(conf);
    baseDir = fs.getHomeDirectory().toString()+"/spmv";      
  }
  
  /**
   * Simple test.
   * multiplying
   *  [1 0 6 0]      [2]    [38]
   *  [0 4 0 0]  *   [3] =  [12]
   *  [0 2 3 0]      [6]    [24]
   *  [3 0 0 5]      [1]    [11]
   */
  @Test
  public void simpleSpMVTest(){
    try {
      HamaConfiguration conf = new HamaConfiguration();
      WritableUtil writableUtil = new WritableUtil();
      String testDir = "/simple/";
      int size = 4;
      
      //creating test matrix
      HashMap<Integer, VectorWritable> inputMatrix = new HashMap<Integer, VectorWritable>();
      SparseVectorWritable vector0 = new SparseVectorWritable();
      vector0.setSize(size);
      vector0.addCell(0, 1);
      vector0.addCell(2, 6);
      SparseVectorWritable vector1 = new SparseVectorWritable();
      vector1.setSize(size);
      vector1.addCell(1, 4);
      SparseVectorWritable vector2 = new SparseVectorWritable();
      vector2.setSize(size);
      vector2.addCell(1, 2);
      vector2.addCell(2, 3);
      SparseVectorWritable vector3 = new SparseVectorWritable();
      vector3.setSize(size);
      vector3.addCell(0, 3);
      vector3.addCell(3, 5);
      inputMatrix.put(0, vector0);
      inputMatrix.put(1, vector1);
      inputMatrix.put(2, vector2);
      inputMatrix.put(3, vector3);
      String matrixPath = baseDir+testDir+"inputMatrix";
      writableUtil.writeMatrix(matrixPath, conf, inputMatrix);
      
      HashMap<Integer, VectorWritable> inputVector = new HashMap<Integer, VectorWritable>();
      DenseVectorWritable vector = new DenseVectorWritable();
      vector.setSize(size);
      vector.addCell(0, 2);
      vector.addCell(1, 3);
      vector.addCell(2, 6);
      vector.addCell(3, 1);
      inputVector.put(0, vector);
      String vectorPath = baseDir+testDir+"inputVector";
      writableUtil.writeMatrix(vectorPath, conf, inputVector);
      
      String outputPath = baseDir+testDir;
      SpMV.setRequestedBspTasksCount(4);
      SpMV.setOutputPath(outputPath);
      SpMV.setInputMatrixPath(matrixPath);
      SpMV.setInputVectorPath(vectorPath);
      SpMV.main(new String[0]);
      
      String resultPath = SpMV.getResultPath();
      DenseVectorWritable result = new DenseVectorWritable();
      writableUtil.readFromFile(resultPath, result, conf);
      
      double expected[] = {38, 12, 24, 11};
      if (result.getSize() != size)
        throw new Exception("Incorrect size of output vector");
      for (int i = 0; i < result.getSize(); i++)
        if ((result.get(i) - expected[i]) < 0.01)
          expected[i] = 0;
      
      for (int i = 0; i < expected.length; i++)
        if (expected[i] != 0)
          throw new Exception("Result doesn't meets expectations");      
      
    } catch (Exception e){
      fail(e.getLocalizedMessage());
    }
  }
}
