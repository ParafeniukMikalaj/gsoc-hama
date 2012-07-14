package org.apache.hama.examples.linearalgebra.util;

import static org.junit.Assert.fail;

import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.junit.Test;

public class RandomMatrixGeneratorTest {
  
  @Test
  public void testRandomMatrixGeneratorEmptyArgs() {
    try {
      RandomMatrixGenerator.main(new String[0]);
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorIncorrectArgs() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-foo=bar", "-s=0.1"});
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      //everything ok
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorIncorrectArgs1() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=-200", "-r=200"});
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      //everything ok
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorIncorrectArgs2() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-s=#"});
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      //everything ok
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorSmallSparse() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=4", "-r=4", "-s=0.1"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorLargeSparse() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=400", "-r=400", "-s=0.1"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorSmallDense() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=4", "-r=4", "-s=0.8"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorLargeDense() {
    try {
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-s=0.8"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorPrecisionSparse(){
    try {
      RandomMatrixGenerator.main(new String[]{"-c=100", "-r=100", "-s=0.1"});
      Matrix matrixFormat = RandomMatrixGenerator.getResult();
      double delta = Math.abs(matrixFormat.getItemsCount()-1000)/1000.0;
      if (delta > 0.05)
        fail("Random matrix generator is not precise enough: needed = 1000 created = "+matrixFormat.getItemsCount());
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorPrecisionDense(){
    try {
      RandomMatrixGenerator.main(new String[]{"-c=100", "-r=100", "-s=0.8"});
      Matrix matrixFormat = RandomMatrixGenerator.getResult();
      double delta = Math.abs(matrixFormat.getItemsCount()-8000)/8000.0;
      if (delta > 0.05)
        fail("Dense matrix generator is not precise enough: needed = 8000 created = "+matrixFormat.getItemsCount());
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
}
