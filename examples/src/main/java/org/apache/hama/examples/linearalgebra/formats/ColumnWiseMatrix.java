package org.apache.hama.examples.linearalgebra.formats;

/**
 * This interface is for matrix formats which can easily provide their column by
 * number.
 */
public interface ColumnWiseMatrix {
  /**
   * Returns matrix format column.
   */
  public SparseVector getColumn(int column);
}
