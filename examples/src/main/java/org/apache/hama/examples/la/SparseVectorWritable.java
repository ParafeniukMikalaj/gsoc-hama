package org.apache.hama.examples.la;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class is used for simplifying work of {@link RandomMatrixGenerator} and
 * {@link SpMV}. Needed for row-wise matrix distribution
 */
public class SparseVectorWritable implements VectorWritable {

  private Integer size;
  private List<Integer> indeces;
  private List<Double> values;
  
  private class SparseVectorIterator implements Iterator<VectorCell>{
    private int index;
    
    public SparseVectorIterator(){
      index = 0;
    }
    
    @Override
    public boolean hasNext() {
      return index < indeces.size();
    }

    @Override
    public VectorCell next() {
      if (!hasNext())
        throw new IllegalStateException("SparseVectorIterator has no more items");
      VectorCell cell = new VectorCell(indeces.get(index), values.get(index));
      index++;
      return cell;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("DenseVectorIterator doesn't permit's item deletion");
    }
    
  }  

  public SparseVectorWritable() {
    indeces = new ArrayList<Integer>();
    values = new ArrayList<Double>();
  }  

  @Override
  public Iterator<VectorCell> getIterator() {
    return new SparseVectorIterator();
  }  

  @Override
  public void addCell(int index, double value) {
    indeces.add(index);
    values.add(value);
  }
  
  public void setSize(int size) {
    this.size = size;
  }
  
  public int getSize(){
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
      st.append("("+indeces.get(i)+ " "+values.get(i)+") ");
    return st.toString();
  }

}
