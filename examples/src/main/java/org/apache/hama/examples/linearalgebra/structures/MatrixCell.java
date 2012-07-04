package org.apache.hama.examples.linearalgebra.structures;

/**
 * This class represents matrix cell. It will be used as common data structure
 * to transmit data.
 */
public class MatrixCell {

  private double value;
  private int row, column;

  /**
   * Constructor with internal initialization.
   */
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

  public void setValue(double value) {
    this.value = value;
  }

  public void setRow(int row) {
    this.row = row;
  }

  public void setColumn(int column) {
    this.column = column;
  }

  @Override
  public String toString() {
    return "(" + row + ", " + column + ")=" + value;
  }

}
