package org.apache.hama.examples.linearalgebra;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class DenseMatrix extends AbstractMatrixFormat {

  private int itemsCount;
  private double[][] data;

  private class DenseMatrixIterator implements Iterator<MatrixCell> {

    private int i, j;

    public DenseMatrixIterator() {
      i = j = 0;
    }

    @Override
    public boolean hasNext() {
      if (i * columns + j < rows * columns)
        return true;
      return false;
    }

    @Override
    public MatrixCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "DenseMatrixIterator has no more elements to iterate");
      MatrixCell current = new MatrixCell(i, j, data[i][j]);
      int oneDimensionIndex = i * columns + j;
      oneDimensionIndex++;
      i = oneDimensionIndex / columns;
      j = oneDimensionIndex % columns;
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "DenseMatrixIterator can't modify underlying collection");
    }

  }

  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new DenseMatrixIterator();
  }

  @Override
  public void setMatrixCell(MatrixCell cell) {
    if (!hasCell(cell.getRow(), cell.getColumn()) && cell.getValue() != 0)
      itemsCount++;
    data[cell.getRow()][cell.getColumn()] = cell.getValue();
  }

  @Override
  public void init() {
    data = new double[rows][columns];
    itemsCount = 0;
  }

  @Override
  public double getCell(int row, int column) {
    return data[row][column];
  }

  @Override
  public boolean hasCell(int row, int column) {
    if (data[row][column] != 0)
      return true;
    return false;
  }

  @Override
  public int getItemsCount() {
    return itemsCount;
  }

}
