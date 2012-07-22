package org.apache.hama.examples.linearalgebra.formats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class contains implementation of Compressed Row Storage(CRS)
 * implementation of MatrixFormat. Web page with explanation of format will be
 * created later.
 */
public class CRSMatrix extends AbstractMatrix {

  private List<Double> values;
  private List<Integer> indeces;
  private int[] start;
  private HashSet<Integer> containedIndeces;
  private HashMap<Integer, Integer> rowIndexMapping, colIndexMapping,
      rowIndexBackMapping, colIndexBackMapping;

  /**
   * Custom cell iterator for this format.
   */
  private class CRSMatrixIterator implements Iterator<MatrixCell> {

    private int index;

    public CRSMatrixIterator() {
      index = 0;
    }

    @Override
    public boolean hasNext() {
      if (getItemsCount() == 0 || index >= values.size())
        return false;
      return true;
    }

    @Override
    public MatrixCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "CRSMatrixIterator has no more elements to iterate");
      int row = getRow(index);
      int column = indeces.get(index);
      double value = values.get(index);
      index++;
      return new MatrixCell(row, column, value);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "CRSMatrixIterator can't modify underlying collection");
    }

    private int getRow(int index) {
      int row = 0;
      for (; row < start.length - 1; row++)
        if (index >= start[row] && index < start[row + 1])
          break;
      return row;
    }

  }

  public CRSMatrix() {
    rowIndexMapping = colIndexMapping = null;
  }

  public CRSMatrix(int rows, int columns) {
    super(rows, columns);
    init();
    rowIndexMapping = colIndexMapping = rowIndexBackMapping = colIndexBackMapping = null;
  }

  private int getGreaterIndex(List<Integer> list, int start, int end, int value) {
    int initialEnd = end;
    while (end - start > 1) {
      int middle = (end + start) / 2;
      if (middle == start || middle == end)
        break;
      if (list.get(middle) < value)
        start = middle;
      else
        end = middle;
    }
    if (start > end)
      return start;
    try {
      for (int i = start; i <= end; i++)
        if (list.get(i) >= value)
          return i;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return initialEnd + 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new CRSMatrixIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void setMatrixCell(MatrixCell cell) {
    super.setMatrixCell(cell);
    boolean inserted = true;
    int row = cell.getRow();
    int column = cell.getColumn();
    double value = cell.getValue();
    int startIndex = 0;
    try {
      startIndex = start[row];
    } catch (Exception e){
      e.printStackTrace();
    }
    int endIndex = start[row + 1];
    int index = getGreaterIndex(indeces, startIndex, endIndex - 1, column);
    if (index < indeces.size() && indeces.get(index) == column) {
      inserted = false;
      values.remove(index);
      indeces.remove(index);
    }
    containedIndeces.add(row * columns + column);
    values.add(index, value);
    indeces.add(index, column);
    if (inserted)
      for (int i = row + 1; i < rows + 1; i++)
        start[i] = start[i] + 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    super.init();
    containedIndeces = new HashSet<Integer>();
    values = new ArrayList<Double>();
    indeces = new ArrayList<Integer>();
    start = new int[rows + 1];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    int startIndex = start[row];
    int endIndex = start[row + 1];
    List<Integer> rowIndeces = indeces.subList(startIndex, endIndex);
    int position = rowIndeces.indexOf(column);
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
    return containedIndeces.contains(row * columns + column);
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
  public SparseVector getRow(int row) {
    SparseVector result = new SparseVector();
    result.setDimension(columns);
    result.init();
    int startIndex = start[row];
    int endIndex = start[row + 1];
    for (int i = startIndex; i < endIndex; i++)
      result.setVectorCell(new VectorCell(indeces.get(i), values.get(i)));
    return result;
  }

  public void compress() {
    rowIndexMapping = new HashMap<Integer, Integer>();
    colIndexMapping = new HashMap<Integer, Integer>();
    rowIndexBackMapping = new HashMap<Integer, Integer>();
    colIndexBackMapping = new HashMap<Integer, Integer>();
    for (int i = 0; i < nonzeroRows.size(); i++) {
      rowIndexMapping.put(nonzeroRows.get(i), i);
      rowIndexBackMapping.put(i, nonzeroRows.get(i));
    }
    for (int i = 0; i < nonzeroColumns.size(); i++) {
      colIndexMapping.put(nonzeroColumns.get(i), i);
      colIndexBackMapping.put(i, nonzeroColumns.get(i));
    }
    for (int i = 0; i < indeces.size(); i++)
      indeces.set(i, colIndexMapping.get(indeces.get(i)));
    int[] compressedStart = new int[nonzeroRows.size() + 1];
    int currentStart = 0;
    for (int i = 1; i < rows + 1; i++) {
      int delta = start[i] - start[i - 1];
      if (delta > 0) {
        compressedStart[currentStart + 1] = compressedStart[currentStart] + delta;
        currentStart++;
      }
    }
    start = compressedStart;
  }

  public HashMap<Integer, Integer> getRowMapping() {
    return rowIndexMapping;
  }

  public HashMap<Integer, Integer> getColumnMapping() {
    return colIndexMapping;
  }

  public HashMap<Integer, Integer> getBackRowMapping() {
    return rowIndexBackMapping;
  }

  public HashMap<Integer, Integer> getBackColumnMapping() {
    return colIndexBackMapping;
  }

  public String toString() {
    return "CRSMatrix [values=" + values + ", indeces=" + indeces + ", start="
        + start + "]";
  }
 
}
