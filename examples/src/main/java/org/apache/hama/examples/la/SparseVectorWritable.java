package org.apache.hama.examples.la;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * This class represents sparse vector. It will give improvement in memory
 * consumption in case of vectors which sparsity is close to zero. Can be used
 * in {@link SpMV} for representing input matrix rows efficiently. Internally
 * represents values as list of indeces and list of values.
 */
public class SparseVectorWritable implements Writable {

  private Integer size;
  private List<Integer> indeces;
  private List<Double> values;

  public SparseVectorWritable() {
    indeces = new ArrayList<Integer>();
    values = new ArrayList<Double>();
  }

  public void addCell(int index, double value) {
    indeces.add(index);
    values.add(value);
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getSize() {
    if (size != null)
      return size;
    return indeces.size();
  }

  public List<Integer> getIndeces() {
    return indeces;
  }

  public List<Double> getValues() {
    return values;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    int len = in.readInt();
    setSize(size);
    for (int i = 0; i < len; i++) {
      int index = in.readInt();
      double value = in.readDouble();
      this.addCell(index, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getSize());
    out.writeInt(indeces.size());
    for (int i = 0; i < indeces.size(); i++) {
      out.writeInt(indeces.get(i));
      out.writeDouble(values.get(i));
    }
  }

  
  @Override
  public String toString() {
    StringBuilder st = new StringBuilder();
    for (int i = 0; i < indeces.size(); i++)
      st.append("(" + indeces.get(i) + " " + values.get(i) + ") ");
    return st.toString();
  }

}
