package org.apache.hama.examples.linearalgebra;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * Interface for every new supported matrix data format.
 */
public interface MatrixFormat extends Writable{

  public Iterator<MatrixCell> getDataIterator();

  public void setMatrixCell(MatrixCell cell);

  public void init();

  public int getRows();

  public int getColumns();

  public void setRows(int rows);

  public void setColumns(int columns);
  
  public double getCell (int row, int column);
  
  public boolean hasCell (int row, int column);

  public abstract int getItemsCount();

  public double getSparsity();

}
