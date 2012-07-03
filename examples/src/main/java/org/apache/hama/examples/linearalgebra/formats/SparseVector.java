package org.apache.hama.examples.linearalgebra.formats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class contains implementation of Sparse Vector Format. Contains
 * list of values and indeces.
 */
public class SparseVector extends AbstractVector {

  private boolean locationInited;
  private List<Double> values;
  private List<Integer> indeces;
  private int[] location;

  /**
   * Custom cell iterator for this format.
   */
  private class SparseVectorIterator implements Iterator<VectorCell> {

    private int index;
    
    public SparseVectorIterator() {
      index = 0;
    }
    
    @Override
    public boolean hasNext() {
      if (index >= values.size())
        return false;
      return true;
    }

    @Override
    public VectorCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "SparseVectorIterator has no more elements to iterate");
      int position = indeces.get(index);
      double value = values.get(index);
      index++;
      return new VectorCell(position, value);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "SparseVectorIterator can't modify underlying collection");
    }

  }
  
  public SparseVector(){
    locationInited = false;
  }
  
  public SparseVector(int dimension){
    super(dimension);
    locationInited = false;
    location = new int[dimension];
    Collections.nCopies(-1, location);    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<VectorCell> getDataIterator() {
    return new SparseVectorIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setVectorCell(VectorCell cell) {
    int position = cell.getPosition();
    double value = cell.getValue();
    int index = indeces.indexOf(position);
    if (index != -1) {
      values.remove(index);
      indeces.remove(index);
    }
    indeces.add(index, position);
    values.add(index, value);
    locationInited = false;
  }

  /**
   * May be one of improvements in work with sparse vector.
   */
  @SuppressWarnings("unused")
  private void initLocation() {
    if (locationInited)
    Collections.nCopies(-1, location);  
    for (int i = 0; i < indeces.size(); i++) 
      location[indeces.get(i)] = i;    
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    values = new ArrayList<Double>();
    indeces = new ArrayList<Integer>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int position) {
    int index = indeces.indexOf(position);
    if (index != -1)
      return values.get(index);
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int position) {
    if (indeces.indexOf(position) != -1)
      return true;
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getItemsCount() {
    return values.size();
  }

}
