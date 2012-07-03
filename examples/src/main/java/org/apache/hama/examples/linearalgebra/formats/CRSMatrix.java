package org.apache.hama.examples.linearalgebra.formats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
public class CRSMatrix extends AbstractMatrix implements RowWiseMatrix,
    ContractibleMatrix {

  private List<Double> values;
  private List<Integer> indeces;
  private List<Integer> start;
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
      double value = indeces.get(index);
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
      for (; row < start.size() - 1; row++)
        if (index >= start.get(row) && index < start.get(row + 1))
          break;
      return row;
    }

  }

  public CRSMatrix() {
    rowIndexMapping = colIndexMapping = null;
  }

  public CRSMatrix(int rows, int columns) {
    super(rows, columns);
    rowIndexMapping = colIndexMapping = rowIndexBackMapping = colIndexBackMapping = null;
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
  public void setMatrixCell(MatrixCell cell) {
    super.setMatrixCell(cell);
    int row = cell.getRow();
    int column = cell.getColumn();
    double value = cell.getValue();
    int startIndex = start.get(row);
    int endIndex = start.get(row + 1);
    int index = startIndex;
    for (int i = startIndex; i < endIndex; i++)
      if (indeces.get(i) >= column) {
        if (indeces.get(i) == column) {
          values.remove(i);
          indeces.remove(i);
        }
        index = i;
        break;
      }
    values.add(index, value);
    indeces.add(index, column);
    for (int i = row + 1; i < rows + 1; i++)
      start.set(i, start.get(i) + 1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    values = new ArrayList<Double>();
    indeces = new ArrayList<Integer>();
    start = new ArrayList<Integer>(Collections.nCopies(rows + 1, 0));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    int startIndex = start.get(row);
    int endIndex = start.get(row + 1);
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
    int startIndex = start.get(row);
    int endIndex = start.get(row + 1);
    List<Integer> rowIndeces = indeces.subList(startIndex, endIndex);
    int position = rowIndeces.indexOf(column);
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
  public SparseVector getRow(int row) {
    SparseVector result = new SparseVector();
    result.setDimension(columns);
    result.init();
    int startIndex = start.get(row);
    int endIndex = start.get(row + 1);
    for (int i = startIndex; i < endIndex; i++)
      result.setVectorCell(new VectorCell(indeces.get(i), values.get(i)));
    return result;
  }

  @Override
  public void compress() {
    Collections.sort(nonzeroRows);
    Collections.sort(nonzeroColumns);
    rowIndexMapping = new HashMap<Integer, Integer>();
    colIndexMapping = new HashMap<Integer, Integer>();
    for (int i = 0; i < nonzeroRows.size(); i++) {
      rowIndexMapping.put(nonzeroRows.get(i), i);
      rowIndexBackMapping.put(i, nonzeroRows.get(i));
    }
    for (int i = 0; i < nonzeroColumns.size(); i++) {
      colIndexMapping.put(nonzeroColumns.get(i), i);
      colIndexMapping.put(i, nonzeroColumns.get(i));
    }
    for (int i = 0; i < indeces.size(); i++)
      indeces.set(i, colIndexMapping.get(indeces.get(i)));
    ArrayList<Integer> compressedStart = new ArrayList<Integer>(
        Collections.nCopies(nonzeroRows.size() + 1, 0));
    for (int i = 1; i < rows + 1; i++) {
      int delta = start.get(i) - start.get(i - 1);
      if (delta > 0)
        compressedStart.set(i, compressedStart.get(i - 1) + delta);
    }
    start = compressedStart;
  }

  @Override
  public HashMap<Integer, Integer> getRowMapping() {
    return rowIndexMapping;
  }

  @Override
  public HashMap<Integer, Integer> getColumnMapping() {
    return colIndexMapping;
  }

  @Override
  public HashMap<Integer, Integer> getBackRowMapping() {
    return rowIndexBackMapping;
  }

  @Override
  public HashMap<Integer, Integer> getBackColumnMapping() {
    return colIndexBackMapping;
  }

}
