package org.apache.hama.examples.linearalgebra.formats;

/**
 * This interface is for matrix formats which can easily provide their column by
 * number.
 */
public interface ColumnWiseMatrixFormat {
  /**
   * Returns matrix format column.
   */
  public SparseVector getColumn(int column);
}
