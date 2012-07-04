package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hama.examples.linearalgebra.structures.LinkedCell;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class contains implementation of Double Linked Matrix Format. Web page
 * with explanation of format will be created later. Items presented with
 * {@link LinkedCell}. Can be easily iterated from left and top edge.
 */
public class DoubleLinkedMatrix extends AbstractMatrix implements
    ColumnWiseMatrix, RowWiseMatrix {

  private int itemsCount;
  private LinkedCell leftStart[];
  private LinkedCell topStart[];

  /**
   * Custom cell iterator for this format.
   */
  private class DoubleLinkedMatrixIterator implements Iterator<MatrixCell> {

    private int currentRow;
    private LinkedCell currentCell;

    public DoubleLinkedMatrixIterator() {
      currentRow = 0;
      nextRow();
    }

    private void nextRow() {
      while (currentRow < rows || leftStart[currentRow] == null)
        currentRow++;
    }

    @Override
    public boolean hasNext() {
      if (currentRow > rows)
        return false;
      return true;
    }

    @Override
    public MatrixCell next() {
      if (!hasNext())
        throw new NoSuchElementException(
            "DoubleLinkedMatrixIterator has no more elements to iterate");
      if (currentCell == null)
        currentCell = leftStart[currentRow];
      MatrixCell result = currentCell.getCell();
      currentCell = currentCell.getRight();
      if (currentCell == null) {
        currentRow++;
        nextRow();
      }
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "DoubleLinkedMatrixIterator can't modify underlying collection");
    }

  }

  public DoubleLinkedMatrix() {
  }

  public DoubleLinkedMatrix(int rows, int columns) {
    super(rows, columns);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<MatrixCell> getDataIterator() {
    return new DoubleLinkedMatrixIterator();
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
    LinkedCell insertedCell = new LinkedCell();
    insertedCell.setCell(cell);
    itemsCount++;

    if (leftStart[row] == null) {
      insertedCell.setRight(null);
      insertedCell.setLeft(null);
      leftStart[row] = insertedCell;
    } else {
      LinkedCell nextCell = leftStart[row];
      while (nextCell.getRight() != null
          && nextCell.getCell().getColumn() < column)
        nextCell = nextCell.getRight();
      if (nextCell.getCell().getColumn() == column) {
        nextCell.getCell().setValue(value);
        itemsCount--;
        return;
      } else if (nextCell.getCell().getColumn() < column) {
        insertedCell.setLeft(nextCell);
        insertedCell.setRight(null);
        nextCell.setRight(insertedCell);
      } else {
        insertedCell.setRight(nextCell);
        insertedCell.setLeft(nextCell.getLeft());
        nextCell.setLeft(insertedCell);
      }
    }

    if (topStart[column] == null) {
      insertedCell.setTop(null);
      insertedCell.setBottom(null);
      topStart[column] = insertedCell;
    } else {
      LinkedCell nextCell = topStart[column];
      while (nextCell.getBottom() != null && nextCell.getCell().getRow() < row)
        nextCell = nextCell.getBottom();
      if (nextCell.getCell().getRow() == row) {
        nextCell.getCell().setValue(value);
      } else if (nextCell.getCell().getRow() < row) {
        insertedCell.setTop(nextCell);
        insertedCell.setBottom(null);
        nextCell.setBottom(insertedCell);
      } else {
        insertedCell.setBottom(nextCell);
        insertedCell.setTop(nextCell.getBottom());
        nextCell.setTop(insertedCell);
      }
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    leftStart = new LinkedCell[rows];
    topStart = new LinkedCell[columns];
    itemsCount = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getCell(int row, int column) {
    if (leftStart[row] == null)
      return 0;
    LinkedCell currentCell = leftStart[row];
    while (currentCell.getRight() != null
        && currentCell.getCell().getColumn() < column)
      currentCell = currentCell.getRight();
    if (currentCell.getCell().getColumn() == column)
      return currentCell.getCell().getValue();
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasCell(int row, int column) {
    if (leftStart[row] == null)
      return false;
    LinkedCell currentCell = leftStart[row];
    while (currentCell.getRight() != null
        && currentCell.getCell().getColumn() < column)
      currentCell = currentCell.getRight();
    if (currentCell.getCell().getColumn() == column)
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
   * {@inheritDoc}
   */
  @Override
  public SparseVector getRow(int row) {
    SparseVector result = new SparseVector();
    result.setDimension(columns);
    result.init();
    LinkedCell currentCell = leftStart[row];
    if (currentCell == null)
      return result;
    while (currentCell != null) {
      int column = currentCell.getCell().getColumn();
      double value = currentCell.getCell().getValue();
      result.setVectorCell(new VectorCell(column, value));
      currentCell = currentCell.getRight();
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SparseVector getColumn(int column) {
    SparseVector result = new SparseVector();
    result.setDimension(rows);
    result.init();
    LinkedCell currentCell = topStart[column];
    if (currentCell == null)
      return result;
    while (currentCell != null) {
      int row = currentCell.getCell().getColumn();
      double value = currentCell.getCell().getValue();
      result.setVectorCell(new VectorCell(row, value));
      currentCell = currentCell.getBottom();
    }
    return result;
  }

}
