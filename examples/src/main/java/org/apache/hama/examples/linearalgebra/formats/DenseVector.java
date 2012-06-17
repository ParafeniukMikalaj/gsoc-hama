package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.VectorCell;

public class DenseVector extends AbstractVectorFormat {

  private double[] data;
  private int itemsCount;

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

  @Override
  public Iterator<VectorCell> getDataIterator() {
    return new DenseVectorIterator();
  }

  @Override
  public void setVectorCell(VectorCell cell) {
    if (!hasCell(cell.getPosition()) && cell.getValue() != 0)
      itemsCount++;
    data[cell.getPosition()] = cell.getValue();
  }

  @Override
  public void init() {
    data = new double[dimension];
  }

  @Override
  public double getCell(int position) {
    return data[position];
  }

  @Override
  public boolean hasCell(int position) {
    if (data[position] != 0)
      return true;
    return false;
  }

  @Override
  public int getItemsCount() {
    return itemsCount;
  }

}
