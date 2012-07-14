package org.apache.hama.examples.linearalgebra.formats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.util.RandomMatrixGenerator;
import org.junit.After;
import org.junit.Before;

public class MatrixFormatTest {
  private String basePath = "/tmp/format-test-";
  private int smallN = 10;
  private int largeN = 1000;
  private Path smallFile, largeFile;
  private HamaConfiguration conf;
  
  private void writeMatrixToFile(Path path, Matrix matrixFormat){
    try {
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] files = fs.listStatus(smallFile);
      for (int i = 0; i < files.length; i++) {
        if (files[i].getLen() > 0) {
          FSDataOutputStream out = fs.create(files[i].getPath());
          out.close();
          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
  
  private void readMatrixFromFile(Path path, Matrix matrixFormat) {
    try {
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] files = fs.listStatus(smallFile);
      for (int i = 0; i < files.length; i++) {
        if (files[i].getLen() > 0) {
          FSDataInputStream in = fs.open(files[i].getPath());
          matrixFormat.readFields(in);
          in.close();
          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Before
  public void createTestFiles(){

    try {
      conf = new HamaConfiguration();
      Matrix matrixFormat;
      long timestamp = System.currentTimeMillis();
      String smallFileName = basePath+"small-"+timestamp;
      smallFile = new Path(smallFileName);
      String largeFileName = basePath+"large-"+timestamp;
      largeFile = new Path(largeFileName);


      RandomMatrixGenerator.setRows(smallN);
      RandomMatrixGenerator.setColumns(smallN);
      RandomMatrixGenerator.main(new String[0]);
      matrixFormat = RandomMatrixGenerator.getResult();
      writeMatrixToFile(smallFile, matrixFormat);
      
      RandomMatrixGenerator.setRows(largeN);
      RandomMatrixGenerator.setColumns(largeN);
      RandomMatrixGenerator.main(new String[0]);
      matrixFormat = RandomMatrixGenerator.getResult();
      writeMatrixToFile(largeFile, matrixFormat);
      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    
  }

  private void genericMatrixTest(Matrix matrixFormat,
      String smallFileName, String largeFileName) {
    readMatrixFromFile(smallFile, matrixFormat);
    Iterator<MatrixCell> iterator = matrixFormat.getDataIterator();
  }
  
  @After
  private void clearTestFiles(){
    
  }
}
