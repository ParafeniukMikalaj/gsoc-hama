package org.apache.hama.examples.linearalgebra.structures;

/**
 * This class represents vector cell. It will be used as common data structure
 * to transmit data.
 */
public class VectorCell {
  private double value;
  private int index;

  public VectorCell(int index, double value) {
    this.index = index;
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public int getIndex() {
    return index;
  }

}
