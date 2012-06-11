package org.apache.hama.examples.linearalgebra;

/**
 * Interface which every matrix converter must implement.
 */
public interface MatrixConverter <F extends MatrixFormat, T extends MatrixFormat> {
  public void convert(F f, T t);
}
