package org.apache.hama.examples.linearalgebra;

/**
 * This class represents vector cell. It will be used as
 * common data structure to transmit data.
 */
public class VectorCell {
  private double value;
  public int position;

  public VectorCell(int position, double value) {
    this.position = position;
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public int getPosition() {
    return position;
  }

}
