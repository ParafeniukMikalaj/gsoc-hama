package org.apache.hama.examples.linearalgebra.formats;

public interface ColumnWiseMatrixFormat {
  public SparseVector getColumn(int column);
}
