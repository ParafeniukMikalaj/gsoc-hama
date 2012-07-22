package org.apache.hama.examples.linearalgebra.formats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.junit.Test;

public class FileMatrixTest {

  @Test
  public void createFileMatrix() throws IOException {
    FileMatrix fmw = new FileMatrix();
    fmw.writeCell(new MatrixCell(0, 0, 1.2));
    fmw.writeCell(new MatrixCell(1, 2, 3.2));
    fmw.finishWriting();
    FileMatrix fmr  = new FileMatrix();
    fmr.setPath(fmw.getPath());
    Iterator<MatrixCell> dataIterator = fmr.getDataIterator();
    while (dataIterator.hasNext()){
      System.out.println(dataIterator.next().toString());
    }    
  }
}
