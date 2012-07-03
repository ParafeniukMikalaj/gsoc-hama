package org.apache.hama.examples.linearalgebra.converters;

import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This class implements converter for vector formats. It works with any format
 * which extends VectorFormat and not exploits internal data structures. It only
 * uses iterator from first format to fill the second one.
 */
public class DefaultVectorConverter<F extends Vector, T extends Vector>
    implements VectorConverter<F, T> {

  /**
   * {@inheritDoc} s and f must be not null to avoid code related to reflection
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
