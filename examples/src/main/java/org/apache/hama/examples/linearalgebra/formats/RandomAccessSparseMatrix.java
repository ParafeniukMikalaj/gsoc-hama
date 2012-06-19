package org.apache.hama.examples.linearalgebra.formats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * This class contains implementation of Random Access Sparse Matrix Format.
 * Internally contains HashMap<Integer, HashMap<Integer, Double>>
 */
public class RandomAccessSparseMatrix extends AbstractMatrixFormat {

  private int itemsCount;
  private HashMap<Integer, HashMap<Integer, Double>> data;

  /**
   * Custom iterator for this format.
   */
  private class RandomAccessSparseMatrixIterator implements
      Iterator<MatrixCell> {

    private int currentRow, currentColumn;
    private Integer rowIndeces[], columnIndeces[];
    private HashMap<Integer, Double> rowMap;

    public RandomAccessSparseMatrixIterator() {
      data.keySet().toArray(rowIndeces);
      rowMap = null;
      currentRow = currentColumn = 0;
      columnIndeces = new Integer[0];
    }

    @Override
    public boolean hasNext() {
      if (currentRow >= rowIndeces.length)
        return false;
      return true;
    }

    @Override
    public MatrixCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "RandomAccessSparseMatrixIterator has no more elements to iterate");
      if (rowMap == null) {
        rowMap = data.get(rowIndeces[currentRow]);
        columnIndeces = rowMap.keySet().toArray(columnIndeces);
        currentColumn = 0;
      }
      int row = currentRow;
      int column = columnIndeces[currentColumn];
      double value = rowMap.get(column);
      MatrixCell result = new MatrixCell(row, column, value);
      currentColumn++;
      if (currentColumn >= columnIndeces.length) {
        currentColumn = 0;
        currentRow++;
        rowMap = null;
      }
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "RandomAccessSparseMatrixIterator can't modify underlying collection");
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new RandomAccessSparseMatrixIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setMatrixCell(MatrixCell cell) {
    int row = cell.getRow();
    int column = cell.getColumn();
    double value = cell.getValue();
    HashMap<Integer, Double> rowValue = data.get(row);
    if (rowValue == null) {
      rowValue = new HashMap<Integer, Double>();
      data.put(row, rowValue);
    }
    if (rowValue.containsKey(column))
      rowValue.remove(column);
    else
      itemsCount++;
    rowValue.put(column, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    data = new HashMap<Integer, HashMap<Integer, Double>>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    HashMap<Integer, Double> rowValue = data.get(row);
    if (rowValue == null)
      return 0;
    Double value = rowValue.get(column);
    if (value != null)
      return value;
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int row, int column) {
    HashMap<Integer, Double> rowValue = data.get(row);
    if (rowValue == null)
      return false;
    if (!rowValue.containsKey(column))
      return false;
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getItemsCount() {
    return itemsCount;
  }

}
