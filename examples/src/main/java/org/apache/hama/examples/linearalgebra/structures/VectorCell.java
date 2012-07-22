package org.apache.hama.examples.linearalgebra.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class represents vector cell. It will be used as common data structure
 * to transmit data.
 */
public class VectorCell implements Writable{
  private double value;
  private int index;

  public VectorCell(){
    
  }
  
  public VectorCell(int index, double value) {
    init(index, value);
  }
  
  private void init(int index, double value) {
    this.index = index;
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "(" + index + ", " + value + ")";
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int index = in.readInt();
    double value = in.readDouble();
    init(index, value);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(index);
    out.writeDouble(value);   
  }
  
  

}
