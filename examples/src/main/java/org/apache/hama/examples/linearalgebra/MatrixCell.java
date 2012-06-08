package org.apache.hama.examples.linearalgebra;

/**
 * This class represents matrix cell. It will be used as
 * common data structure to transmit data.
 */
public class MatrixCell {
  
  private double value;
  public int row;
  public int column;

  public MatrixCell(int row, int column, double value) {
    this.row = row;
    this.column = column;
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }
  
}
