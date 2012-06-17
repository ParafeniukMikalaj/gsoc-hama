package org.apache.hama.examples.linearalgebra.converters;

import org.apache.hama.examples.linearalgebra.formats.VectorFormat;


/**
 * Base interface which must be implemented by all converters.
 */
public interface VectorConverter <F extends VectorFormat, T extends VectorFormat> {
  public void convert(F f, T t);
}
