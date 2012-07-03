package org.apache.hama.examples.linearalgebra.formats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * This class contains implementation of Jagged Diagonal Storage (JDS) Matrix
 * Format. Web page with explanation of format will be created later.
 */
public class JDSMatrix extends AbstractMatrix {

  private HashMap<Integer, List<Double>> values;
  private HashMap<Integer, List<Integer>> indeces;
  private int permutation[];
  private int itemsCount;

  private class JDSMatrixIterator implements Iterator<MatrixCell> {

    private Integer rowIndeces[];
    private int rowIndex, columnIndex, prevRow;
    private List<Integer> currentIndeces;
    private List<Double> currentValues;

    /**
     * Custom cell iterator for this format.
     */
    public JDSMatrixIterator() {
      indeces.keySet().toArray(rowIndeces);
      rowIndex = columnIndex = 0;
      prevRow = -1;
    }

    @Override
    public boolean hasNext() {
      if (rowIndex >= rowIndeces.length)
        return false;
      return true;
    }

    @Override
    public MatrixCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "JDSMatrixIterator has no more elements to iterate");
      if (prevRow != rowIndex) {
        int row = rowIndeces[rowIndex];
        currentIndeces = indeces.get(row);
        currentValues = values.get(row);
      }
      int row = rowIndeces[rowIndex];
      int column = currentIndeces.get(columnIndex);
      double value = currentValues.get(rowIndex);
      columnIndex++;
      if (columnIndex >= currentIndeces.size()) {
        columnIndex = 0;
        rowIndex++;
      }
      return new MatrixCell(row, column, value);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "JDSMatrixIterator can't modify underlying collection");
    }

  }
  
  public JDSMatrix(){
  }
  
  public JDSMatrix(int rows, int columns){
    super(rows, columns);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new JDSMatrixIterator();
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
    if (!values.containsKey(row)) {
      values.put(row, new ArrayList<Double>());
      indeces.put(row, new ArrayList<Integer>());
    }
    List<Integer> rowIndeces = indeces.get(row);
    List<Double> rowValues = values.get(row);
    int valueIndex = rowIndeces.indexOf(column);
    if (valueIndex != 0) {
      rowValues.remove(valueIndex);
      rowIndeces.remove(valueIndex);
    } else {
      itemsCount++;
    }
    rowValues.add(value);
    rowIndeces.add(column);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    values = new HashMap<Integer, List<Double>>();
    indeces = new HashMap<Integer, List<Integer>>();
    permutation = new int[rows];
    itemsCount = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    if (!values.containsKey(row))
      return 0;
    List<Integer> rowIndeces = indeces.get(row);
    int index = rowIndeces.indexOf(column);
    if (index == -1)
      return 0;
    List<Double> rowValues = values.get(row);
    return rowValues.get(index);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int row, int column) {
    if (!values.containsKey(row))
      return false;
    List<Integer> rowIndeces = indeces.get(row);
    int index = rowIndeces.indexOf(column);
    if (index == -1)
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

  public void reorder() {
    TreeMap<Integer, Integer> frequency = new TreeMap<Integer, Integer>();
    for (Integer row : indeces.keySet())
      frequency.put(row, indeces.get(row).size());
    for (int i = 0; i < rows; i++)
      permutation[i] = -1;
    int i = 0;
    // FIXME Check if values are sorted properly.
    for (Integer row : frequency.keySet()) {
      permutation[i++] = row;
    }
  }

  /**
   * this method gets diagonal specified by diagonalNumber.
   */
  public List<MatrixCell> getDiagonal(int diagonalNumber) {
    List<MatrixCell> result = new ArrayList<MatrixCell>();
    for (int i = 0; i < indeces.size(); i++) {
      int index = permutation[i];
      List<Double> currentValues = values.get(index);
      List<Integer> currentIndeces = indeces.get(index);
      if (currentValues.size() < diagonalNumber)
        break;
      int row = index;
      int column = currentIndeces.get(diagonalNumber);
      double value = currentValues.get(diagonalNumber);
      MatrixCell currentCell = new MatrixCell(row, column, value);
      result.add(currentCell);
    }
    return result;
  }

}
