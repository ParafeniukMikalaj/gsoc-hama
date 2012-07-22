package org.apache.hama.examples.linearalgebra;

import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.linearalgebra.formats.CRSMatrix;
import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;
import org.apache.hama.examples.linearalgebra.util.RandomMatrixGenerator;
import org.jfree.util.Log;
import org.junit.Test;

public class LAMathTest {

  /**
   * Simple test.
   * multiplying
   *  [1 0 6 0]      [2]    [38]
   *  [0 4 0 0]  *   [3] =  [12]
   *  [0 2 3 0]      [6]    [24]
   *  [3 0 0 5]      [1]    [11]
   */
  /*@Test
  public void testSpMVBasic() {
    try {
      CRSMatrix matrix = new CRSMatrix(4, 4);
      DenseVector vector = new DenseVector(4);
      double[][] matrixData = new double[4][4];

      matrixData[0][0] = 1;
      matrixData[1][1] = 4;
      matrixData[2][2] = 3;
      matrixData[3][3] = 5;
      matrixData[2][1] = 2;
      matrixData[3][0] = 3;
      matrixData[0][2] = 6;
      
      matrix.setData(matrixData);
      double[] vectorData = { 2, 3, 6, 1 };
      vector.setData(vectorData);
      LAMath math = new LAMath();
      LAMath.setRequestedBspTasksCount(4);
      Vector v = math.multiply(matrix, vector);
      System.out.println("Answer is " + v.toString());
      System.out.flush();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testSpMVSmall() {
    try {
      int size = 4;
      double sparsity = 0.4;
      RandomMatrixGenerator.setConfiguration(new HamaConfiguration());
      RandomMatrixGenerator.setRows(size);
      RandomMatrixGenerator.setColumns(size);
      RandomMatrixGenerator.setSparsity((float)sparsity);
      RandomMatrixGenerator.setRequestedBspTasksCount(2);
      RandomMatrixGenerator.main(new String[0]);
      Matrix matrix = RandomMatrixGenerator.getResult();
      RandomMatrixGenerator.setRows(1);
      RandomMatrixGenerator.setColumns(size);
      RandomMatrixGenerator.setSparsity(0.9f);
      RandomMatrixGenerator.setRequestedBspTasksCount(2);
      RandomMatrixGenerator.main(new String[0]);
      Matrix mVector = RandomMatrixGenerator.getResult();
      Iterator<MatrixCell> mVectorInterator = mVector.getDataIterator();
      DenseVector vector = new DenseVector(size);
      while (mVectorInterator.hasNext()) {
        MatrixCell cell = mVectorInterator.next();
        vector.setVectorCell(new VectorCell(cell.getColumn(), cell.getValue()));
      }
      LAMath math = new LAMath();
      LAMath.setRequestedBspTasksCount(4);
      Vector v = math.multiply(matrix, vector);
      Log.debug("Answer is " + v.toString());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testSpMVLarge() {
    try {
      int size = 100;
      double sparsity = 0.2;
      RandomMatrixGenerator.setConfiguration(new HamaConfiguration());     
      RandomMatrixGenerator.setRows(size);
      RandomMatrixGenerator.setColumns(size);
      RandomMatrixGenerator.setSparsity((float)sparsity);
      RandomMatrixGenerator.main(new String[0]);
      Matrix matrix = RandomMatrixGenerator.getResult();
      RandomMatrixGenerator.setRows(1);
      RandomMatrixGenerator.setSparsity(0.9f);
      RandomMatrixGenerator.main(new String[0]);
      Matrix mVector = RandomMatrixGenerator.getResult();
      Iterator<MatrixCell> mVectorInterator = mVector.getDataIterator();
      DenseVector vector = new DenseVector(size);
      while (mVectorInterator.hasNext()) {
        MatrixCell cell = mVectorInterator.next();
        vector.setVectorCell(new VectorCell(cell.getColumn(), cell.getValue()));
      }
      LAMath math = new LAMath();
      LAMath.setRequestedBspTasksCount(4);
      Vector v = math.multiply(matrix, vector);
      Log.debug("Answer is " + v.toString());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getLocalizedMessage());
    }
  }*/
}
