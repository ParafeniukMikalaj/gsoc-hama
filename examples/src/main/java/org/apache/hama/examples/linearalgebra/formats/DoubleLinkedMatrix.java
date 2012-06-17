package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * Will be implemented in few days.
 */
public class DoubleLinkedMatrix extends AbstractMatrixFormat implements
    ColumnWiseMatrixFormat, RowWiseMatrixFormat {

  @Override
  public Iterator<MatrixCell> getDataIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setMatrixCell(MatrixCell cell) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void init() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public double getCell(int row, int column) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean hasCell(int row, int column) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getItemsCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public SparseVector getRow(int row) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SparseVector getColumn(int column) {
    // TODO Auto-generated method stub
    return null;
  }

}
