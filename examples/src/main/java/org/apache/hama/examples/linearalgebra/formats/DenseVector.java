package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class contains implementation of Dense Vector Format. Contains
 * one-dimensioal array of double.
 */
public class DenseVector extends AbstractVectorFormat {

  private double[] data;
  private int itemsCount;

  /**
   * Custom cell iterator for this format.
   */
  private class DenseVectorIterator implements Iterator<VectorCell> {

    private int i;

    public DenseVectorIterator() {
      i = 0;
    }

    @Override
    public boolean hasNext() {
      if (i < dimension)
        return true;
      return false;
    }

    @Override
    public VectorCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "DenseVectorIterator has no more elements to iterate");
      VectorCell current = new VectorCell(i, data[i]);
      i++;
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "DenseVectorIterator can't modify underlying collection");
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<VectorCell> getDataIterator() {
    return new DenseVectorIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setVectorCell(VectorCell cell) {
    if (!hasCell(cell.getPosition()) && cell.getValue() != 0)
      itemsCount++;
    data[cell.getPosition()] = cell.getValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    data = new double[dimension];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int position) {
    return data[position];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int position) {
    if (data[position] != 0)
      return true;
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getItemsCount() {
    return itemsCount;
  }

}
