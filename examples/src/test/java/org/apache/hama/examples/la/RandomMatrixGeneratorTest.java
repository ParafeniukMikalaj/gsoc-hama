package org.apache.hama.examples.la;

import static org.junit.Assert.fail;

import org.junit.Test;

public class RandomMatrixGeneratorTest {
  //@Test
  public void testRandomMatrixGeneratorEmptyArgs() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[0]);
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  //@Test
  public void testRandomMatrixGeneratorIncorrectArgs() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-foo=bar", "-s=0.1"});
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      //everything ok
    }
  }
  
  //@Test
  public void testRandomMatrixGeneratorIncorrectArgs1() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=-200", "-r=200"});
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      //everything ok
    }
  }
  
  //@Test
  public void testRandomMatrixGeneratorIncorrectArgs2() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-s=#"});
      fail("Matrix generator should fail because of invalid arguments.");
    } catch (Exception e) {
      //everything ok
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorSmallSparse() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=4", "-r=4", "-s=0.1", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorLargeSparse() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=10000", "-r=10000", "-s=0.1", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorSmallDense() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=4", "-r=4", "-s=0.8", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  @Test
  public void testRandomMatrixGeneratorLargeDense() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-s=0.8", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
}
