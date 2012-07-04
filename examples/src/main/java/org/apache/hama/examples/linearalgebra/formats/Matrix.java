package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * Interface for every new supported matrix data format.
 */
public interface Matrix extends Writable {

  /**
   * Method returns cell iterator.
   */
  public Iterator<MatrixCell> getDataIterator();

  /**
   * Set matrix cell by row and column from cell. Value is also gained from
   * cell.
   */
  public void setMatrixCell(MatrixCell cell);

  /**
   * User should call this method after setting matrix rows and columns. It is a
   * good place data allocation and initialization.
   */
  public void init();

  /**
   * Returns number of rows.
   */
  public int getRows();

  /**
   * Returns number of columns.
   */
  public int getColumns();

  /**
   * Sets number of rows.
   */
  public void setRows(int rows);

  /**
   * Sets number of columns.
   */
  public void setColumns(int columns);

  /**
   * Get cell by row and column index.
   */
  public double getCell(int row, int column);

  /**
   * Check if cell with row and column index presented in matrix format.
   * 
   * @return true if cell is presented, false otherwise.
   */
  public boolean hasCell(int row, int column);

  /**
   * @return number of non-zero cells in format.
   */
  public abstract int getItemsCount();

  /**
   * Counts sparsity of format.
   */
  public double getSparsity();

}
