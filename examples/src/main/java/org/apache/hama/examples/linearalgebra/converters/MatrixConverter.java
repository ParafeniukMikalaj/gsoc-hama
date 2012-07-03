package org.apache.hama.examples.linearalgebra.converters;

import org.apache.hama.examples.linearalgebra.formats.Matrix;

/**
 * Interface which every matrix converter must implement.
 */
public interface MatrixConverter<F extends Matrix, T extends Matrix> {
  /**
   * Method to convert one matrix format to another.
   * 
   * @param f
   *          - Format from which matrix will be converted. Must implements
   *          {@link Matrix}.
   * @param t
   *          - Format to which matrix will be converted. Must implements
   *          {@link Matrix}.
   */
  public void convert(F f, T t);
}
