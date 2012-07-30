package org.apache.hama.examples.la;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class DenseVectorWritable implements VectorWritable{

  private double values[];
  
  private class DenseVectorIterator implements Iterator<VectorCell>{
    private int index;
    
    public DenseVectorIterator(){
      index = 0;
    }
    
    @Override
    public boolean hasNext() {
      while (index < getSize() && values[index] != 0)
        index++;
      return index < getSize();
    }

    @Override
    public VectorCell next() {
      if (!hasNext())
        throw new IllegalStateException("DenseVector iterator has no more items");
      VectorCell cell = new VectorCell(index, values[index]);
      index++;
      return cell;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("DenseVectorIterator doesn't permit's item deletion");
    }
    
  }  
  
  public DenseVectorWritable(){
    
  }

  @Override
  public Iterator<VectorCell> getIterator() {
    return new DenseVectorIterator();
  }
  
  public int getSize(){
    return values.length;
  }
  
  public void setSize(int size){
    values = new double[size];
  }
  
  public double get(int index) {
    return values[index];
  }  

  @Override
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
