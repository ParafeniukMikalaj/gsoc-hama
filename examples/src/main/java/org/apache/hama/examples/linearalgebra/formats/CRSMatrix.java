package org.apache.hama.examples.linearalgebra.formats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

public class CRSMatrix extends AbstractMatrixFormat implements RowWiseMatrixFormat{
  
  private List<Double> values;
  private List<Integer> indeces;
  private List<Integer> start;
  
  private class CRSMatrixIterator implements Iterator<MatrixCell> {
    
    private int index;
    
    public CRSMatrixIterator() {
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
    
    private int getRow(int index){
      int row = 0;
      for (;row < start.size() - 1; row++)
        if (index >= start.get(row) && index < start.get(row+1))
          break;
      return row;
    }
    
  }

  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new CRSMatrixIterator();
  }

  @Override
  public void setMatrixCell(MatrixCell cell) {
    int row = cell.getRow();
    int column = cell.getColumn();
    double value = cell.getValue();
    int startIndex = start.get(row);
    int endIndex = start.get(row+1);
    int index = startIndex;
    for (int i = startIndex; i < endIndex; i++)
      if (indeces.get(i) >= column) {
        if (indeces.get(i) == column)
          values.remove(i);
        index = i;
        break;
      }
    values.add(index, value);
    indeces.add(index, column);
    for (int i = row + 1; i < rows + 1; i++)
      start.set(i, start.get(i) + 1);
  }

  @Override
  public void init() {
    values = new ArrayList<Double>();
    indeces = new ArrayList<Integer>();
    start = new ArrayList<Integer>(rows + 1); 
  }

  @Override
  public double getCell(int row, int column) {
    int startIndex = start.get(row);
    int endIndex = start.get(row+1);
    List<Integer> rowIndeces = indeces.subList(startIndex, endIndex);
    int position = rowIndeces.indexOf(column);
    if (position != -1)
      return 0;
    position += startIndex;
    return values.get(position);  
  }

  @Override
  public boolean hasCell(int row, int column) {
    int startIndex = start.get(row);
    int endIndex = start.get(row+1);
    List<Integer> rowIndeces = indeces.subList(startIndex, endIndex);
    int position = rowIndeces.indexOf(column);
    if (position != -1)
      return true;
    return false;    
  }

  @Override
  public int getItemsCount() {
    return values.size();
  }
  
  @Override
  public SparseVector getRow(int row) {
    SparseVector result = new SparseVector();
    result.setDimension(columns);
    result.init();
    int startIndex = start.get(row);
    int endIndex = start.get(row+1);
    for (int i = startIndex; i < endIndex; i++) 
      result.setVectorCell(new VectorCell(indeces.get(i), values.get(i)));
    return result;
  }

}
