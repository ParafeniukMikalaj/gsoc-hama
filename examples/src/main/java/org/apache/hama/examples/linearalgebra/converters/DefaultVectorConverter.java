package org.apache.hama.examples.linearalgebra.converters;

import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.formats.VectorFormat;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class implements converter for vector formats.
 * It works with any format which extends VectorFormat
 * and not exploits internal data structures.
 */
public class DefaultVectorConverter<F extends VectorFormat, T extends VectorFormat>
    implements VectorConverter<F, T> {

  /**
   * s and f must be not null to avoid code related to reflection
   */
  @Override
  public void convert(F fromFormat, T toFormat) {
    if (toFormat == null || fromFormat == null)
      throw new IllegalArgumentException(
          "ERROR in convertion. In and out Structures must be initialized");
    VectorCell cell = null;
    int dimenstion = fromFormat.getDimension();
    toFormat.setDimension(dimenstion);
    toFormat.init();
    Iterator<VectorCell> cellIterator = fromFormat.getDataIterator();
    while (cellIterator.hasNext()) {
      cell = cellIterator.next();
      toFormat.setVectorCell(cell);
    }
  }

}
