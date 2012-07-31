package org.apache.hama.examples.la;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * This class represents dense vector. It will improve memory consumption up to
 * two times in comparison to {@link SparseVectorWritable} in case of vectors
 * which sparsity is close to 1. Internally represents vector values as array.
 * Can be used in {@link SpMV} for representation of input and output vector.
 */
public class DenseVectorWritable implements Writable {

  private double values[];

  public DenseVectorWritable() {

  }

  public int getSize() {
    return values.length;
  }

  public void setSize(int size) {
    values = new double[size];
  }

  public double get(int index) {
    return values[index];
  }

  public void addCell(int index, double value) {
    values[index] = value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    int len = in.readInt();
    setSize(size);
    for (int i = 0; i < len; i++) {
      int index = in.readInt();
      double value = in.readDouble();
      values[index] = value;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getSize());
    out.writeInt(getSize());
    for (int i = 0; i < getSize(); i++) {
      out.writeInt(i);
      out.writeDouble(values[i]);
    }
  }

  @Override
  public String toString() {
    return "values=" + Arrays.toString(values);
  }

}
