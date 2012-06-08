package org.apache.hama.examples.linearalgebra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Interface for every new supported matrix data format.
 */
public interface MatrixFormat {

  public abstract Iterator<MatrixCell> getDataIterator();

  public abstract void setMatrixCell(MatrixCell cell);

  public abstract void init();

  public int getRows();

  public int getColumns();

  public void setRows(int rows);

  public void setColumns(int columns);
  
  public double getCell (int row, int column);
  
  public boolean hasCell (int row, int column);

  public abstract int getItemsCount();

  public double getSparsity();
  
  public void writeMatrix(DataOutput out) throws IOException;
  
  public void readMatrix(DataInput in) throws IOException;

}
