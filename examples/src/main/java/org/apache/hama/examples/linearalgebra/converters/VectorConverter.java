package org.apache.hama.examples.linearalgebra.converters;

import org.apache.hama.examples.linearalgebra.formats.Vector;

/**
 * Base interface which must be implemented by all converters.
 */
public interface VectorConverter<F extends Vector, T extends Vector> {
  /**
   * Method to convert one vector format to another.
   * 
   * @param f
   *          - Format from which vector will be converted. Must implement
   *          {@link Vector}
   * @param t
   *          - Format to whick vector will be converted. Must implement
   *          {@link Vector}
   */
  public void convert(F f, T t);
}
