package org.apache.hama.examples.linearalgebra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public interface VectorFormat {
  
  public abstract Iterator<VectorCell> getDataIterator();

  public abstract void setVectorCell(VectorCell cell);

  public abstract void init();

  public int getDimension();

  public void setDimension(int rows);
  
  public double getCell (int position);
  
  public boolean hasCell (int position);

  public abstract int getItemsCount();

  public double getSparsity();
  
  public void writeVector(DataOutput out) throws IOException;
  
  public void readVector(DataInput in) throws IOException;
  
}
