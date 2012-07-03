package org.apache.hama.examples.linearalgebra.formats;

/**
 * This interface is for matrix formats which can easily provide their row by
 * number.
 */
public interface RowWiseMatrix {
  /**
   * Returns matrix format row.
   */
  public SparseVector getRow(int row);
}
