package org.apache.hama.examples.la;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.HamaConfiguration;
import org.junit.Test;

public class RandomMatrixGeneratorTest {
  
  private class MatrixReader {
    private String pathString;
    public MatrixReader(String pathString) {
      this.pathString = pathString;
    }
    
    public void read() throws IOException{
      HamaConfiguration conf = new HamaConfiguration();
      Path dir = new Path(pathString);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stats = fs.listStatus(dir); 
      for(FileStatus stat : stats) 
      { 
          String filePath = stat.getPath().toUri().getPath(); // gives directory name 
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(filePath), conf);
          IntWritable key = new IntWritable();
          SparseVectorWritable value = new SparseVectorWritable();
          while (reader.next(key, value)) {
            System.out.print(key.toString());
            System.out.println(value.toString());
          }
      } 
      
    }
  }
  
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
      RandomMatrixGenerator.main(new String[]{"-c=5", "-r=5", "-s=0.3", "-n=4"});
      System.out.println("Generated count = " + RandomMatrixGenerator.getGeneratedCount());
      String outputPath = RandomMatrixGenerator.getOutputPath();
      MatrixReader reader = new MatrixReader(outputPath);
      reader.read();
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  //@Test
  public void testRandomMatrixGeneratorLargeSparse() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=10000", "-r=10000", "-s=0.1", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  //@Test
  public void testRandomMatrixGeneratorSmallDense() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=4", "-r=4", "-s=0.8", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
  //@Test
  public void testRandomMatrixGeneratorLargeDense() {
    try {
      RandomMatrixGenerator.setConfiguration(null);
      RandomMatrixGenerator.main(new String[]{"-c=200", "-r=200", "-s=0.8", "-n=4"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }
  
}
