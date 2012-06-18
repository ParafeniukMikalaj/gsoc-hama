package org.apache.hama.examples.linearalgebra.converters;

import org.apache.hama.examples.linearalgebra.formats.MatrixFormat;

/**
 * Interface which every matrix converter must implement.
 */
public interface MatrixConverter<F extends MatrixFormat, T extends MatrixFormat> {
  /**
   * Method to convert one matrix format to another.
   * 
   * @param f
   *          - Format from which matrix will be converted. Must implements
   *          {@link MatrixFormat}.
   * @param t
   *          - Format to which matrix will be converted. Must implements
   *          {@link MatrixFormat}.
   */
  public void convert(F f, T t);
}
