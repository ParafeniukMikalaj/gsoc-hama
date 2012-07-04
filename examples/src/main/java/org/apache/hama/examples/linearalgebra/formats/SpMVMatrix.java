package org.apache.hama.examples.linearalgebra.formats;

import java.util.List;

/**
 * This interface is used to ease work with spmv. It gives the ability to work
 * only with non-zero rows and columns.
 */
public interface SpMVMatrix {

  /**
   * Returns list of non-zero rows of matrix.
   */
  public List<Integer> getNonZeroRows();

  /**
   * Returns list of non-zero columns of matrix.
   */
  public List<Integer> getNonZeroColumns();
}
