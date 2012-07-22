package org.apache.hama.examples.linearalgebra.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class represents matrix cell. It will be used as common data structure
 * to transmit data.
 */
public class MatrixCell implements Writable {

  private double value;
  private int row, column;

  public MatrixCell() {

  }

  /**
   * Constructor with internal initialization.
   */
  public MatrixCell(int row, int column, double value) {
    init(row, column, value);
  }

  private void init(int row, int column, double value) {
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

  @Override
  public void readFields(DataInput in) throws IOException {
    int cellRow = in.readInt();
    int cellColumn = in.readInt();
    double cellValue = in.readDouble();
    init(cellRow, cellColumn, cellValue);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getRow());
    out.writeInt(getColumn());
    out.writeDouble(getValue());
  }

}
