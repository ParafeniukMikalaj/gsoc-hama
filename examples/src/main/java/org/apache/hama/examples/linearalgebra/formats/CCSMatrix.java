package org.apache.hama.examples.linearalgebra.formats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class contains implementation of Compressed Column Storage(CCS)
 * implementation of MatrixFormat. Web page with explanation of format will be
 * created later.
 */
public class CCSMatrix extends AbstractMatrix implements
    ColumnWiseMatrix {

  private List<Double> values;
  private List<Integer> indeces;
  private List<Integer> start;

  /**
   * Custom cell iterator for this format.
   */
  private class CCSMatrixIterator implements Iterator<MatrixCell> {

    private int index;

    public CCSMatrixIterator() {
      index = 0;
    }

    @Override
    public boolean hasNext() {
      if (getItemsCount() == 0 || index > values.size())
        return false;
      return true;
    }

    @Override
    public MatrixCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "CCSMatrixIterator has no more elements to iterate");
      int column = getColumn(index);
      int row = indeces.get(index);
      double value = indeces.get(index);
      index++;
      return new MatrixCell(row, column, value);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "CCSMatrixIterator can't modify underlying collection");
    }

    private int getColumn(int index) {
      int column = 0;
      for (; column < start.size() - 1; column++)
        if (index >= start.get(column) && index < start.get(column + 1))
          break;
      return column;
    }

  }

  public CCSMatrix() {
  }

  public CCSMatrix(int rows, int columns) {
    super(rows, columns);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new CCSMatrixIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setMatrixCell(MatrixCell cell) {
    super.setMatrixCell(cell);
    int row = cell.getRow();
    int column = cell.getColumn();
    double value = cell.getValue();
    int startIndex = start.get(column);
    int endIndex = start.get(column + 1);
    int index = startIndex;
    for (int i = startIndex; i < endIndex; i++)
      if (indeces.get(i) >= row) {
        if (indeces.get(i) == row)
          values.remove(i);
        index = i;
        break;
      }
    values.add(index, value);
    indeces.add(index, column);
    for (int i = column + 1; i < columns + 1; i++)
      start.set(i, start.get(i) + 1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    values = new ArrayList<Double>();
    indeces = new ArrayList<Integer>();
    start = new ArrayList<Integer>(Collections.nCopies(columns+1, 0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    int startIndex = start.get(column);
    int endIndex = start.get(column + 1);
    List<Integer> rowIndeces = indeces.subList(startIndex, endIndex);
    int position = rowIndeces.indexOf(row);
    if (position != -1)
      return 0;
    position += startIndex;
    return values.get(position);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int row, int column) {
    int startIndex = start.get(column);
    int endIndex = start.get(column + 1);
    List<Integer> columnIndeces = indeces.subList(startIndex, endIndex);
    int position = columnIndeces.indexOf(row);
    if (position != -1)
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

  /**
   * {@inheritDoc}
   */
  @Override
  public SparseVector getColumn(int column) {
    SparseVector result = new SparseVector();
    result.setDimension(columns);
    result.init();
    int startIndex = start.get(column);
    int endIndex = start.get(column + 1);
    for (int i = startIndex; i < endIndex; i++)
      result.setVectorCell(new VectorCell(indeces.get(i), values.get(i)));
    return result;
  }

}
