package org.apache.hama.examples.la;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * This class is used for simplifying work of {@link RandomMatrixGenerator} and
 * {@link SpMV}. Needed for row-wise matrix distribution
 */
public class MatrixRowWritable implements Writable {

  private List<Integer> indeces;
  private List<Double> values;

  public MatrixRowWritable() {
    indeces = new ArrayList<Integer>();
    values = new ArrayList<Double>();
  }

  public void addCell(int index, double value) {
    indeces.add(index);
    values.add(value);
  }
  
  public int size(){
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
    int len = in.readInt();
    for (int i = 0; i < len; i++) {
      int index = in.readInt();
      double value = in.readDouble();
      this.addCell(index, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(indeces.size());
    for (int i = 0; i < indeces.size(); i++) {
      out.writeInt(indeces.get(i));
      out.writeDouble(values.get(i));
    }
  }

}
