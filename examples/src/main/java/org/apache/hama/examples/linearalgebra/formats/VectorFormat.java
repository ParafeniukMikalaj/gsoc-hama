package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * Base interface for all supported vector formats.
 */
public interface VectorFormat extends Writable{
  
  public Iterator<VectorCell> getDataIterator();

  public void setVectorCell(VectorCell cell);

  public void init();

  public int getDimension();

  public void setDimension(int rows);
  
  public double getCell (int position);
  
  public boolean hasCell (int position);

  public abstract int getItemsCount();

  public double getSparsity();
  
}
