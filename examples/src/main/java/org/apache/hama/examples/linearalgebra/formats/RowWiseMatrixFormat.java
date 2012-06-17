package org.apache.hama.examples.linearalgebra.formats;

public interface RowWiseMatrixFormat {
  public SparseVector getRow(int row);
}
