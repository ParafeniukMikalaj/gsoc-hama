package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * This class contains implementation of Dense Matrix Format. Contains
 * two-dimensioal array of double.
 */
public class DenseMatrix extends AbstractMatrixFormat {

  private int itemsCount;
  private double[][] data;

  /**
   * Custom cell iterator for this format.
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new DenseMatrixIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setMatrixCell(MatrixCell cell) {
    if (!hasCell(cell.getRow(), cell.getColumn()) && cell.getValue() != 0)
      itemsCount++;
    data[cell.getRow()][cell.getColumn()] = cell.getValue();
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    data = new double[rows][columns];
    itemsCount = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    return data[row][column];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int row, int column) {
    if (data[row][column] != 0)
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

  /**
   * This method is needed for test purposes.
   */
  public double[][] getData() {
    return data.clone();
  }

  @Override
  public boolean equals(Object other){
    if (other instanceof DenseMatrix) {
      DenseMatrix otherMatrix = (DenseMatrix) other;
      if (rows != otherMatrix.getRows() || columns != otherMatrix.getColumns())
        return false;
      double[][] otherData = otherMatrix.getData();
      for (int i = 0; i < rows; i++)
        for (int j = 0; j < columns; j++)
          if (otherData[i][j] != data[i][j])
            return false;
      return true;
    } else {
      return false;
    }
  }
}
