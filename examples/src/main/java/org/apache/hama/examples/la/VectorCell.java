package org.apache.hama.examples.la;

public class VectorCell {
  
  private int index;
  private double value;

  public VectorCell() {

  }

  public VectorCell(int index, double value) {
    this.index = index;
    this.value = value;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

}
