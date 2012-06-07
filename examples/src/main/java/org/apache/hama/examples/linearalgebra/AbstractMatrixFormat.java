package org.apache.hama.examples.linearalgebra;

import java.util.Iterator;

public abstract class AbstractMatrixFormat {
  private int rows, columns;

  public abstract Iterator<MatrixCell> getDataIterator();

  public abstract void setMatrixCell(MatrixCell cell);
  
  public abstract void init();

  public AbstractMatrixFormat(int rows, int columns) {
    this.rows = rows;
    this.columns = columns;
    init();
  }
  
  public int getRows() {
    return rows;
  }

  public int getColumns() {
    return columns;
  }
  
  public void setRows(int rows) {
    this.rows = rows;
  }

  public void setColumns(int columns) {
    this.columns = columns;
  }
  
  // this method will return number of non-zero items
  public abstract int getItemsCount();

  public double getSparsity() {
    return ((double) getItemsCount()) / (rows * columns);
  }
}
