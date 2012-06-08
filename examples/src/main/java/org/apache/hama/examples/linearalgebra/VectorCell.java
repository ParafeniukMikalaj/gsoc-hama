package org.apache.hama.examples.linearalgebra;

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
