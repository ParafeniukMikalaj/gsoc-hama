package org.apache.hama.examples.linearalgebra.converters;

import org.apache.hama.examples.linearalgebra.formats.VectorFormat;

/**
 * Base interface which must be implemented by all converters.
 */
public interface VectorConverter<F extends VectorFormat, T extends VectorFormat> {
  /**
   * Method to convert one vector format to another.
   * 
   * @param f
   *          - Format from which vector will be converted. Must implement
   *          {@link VectorFormat}
   * @param t
   *          - Format to whick vector will be converted. Must implement
   *          {@link VectorFormat}
   */
  public void convert(F f, T t);
}
