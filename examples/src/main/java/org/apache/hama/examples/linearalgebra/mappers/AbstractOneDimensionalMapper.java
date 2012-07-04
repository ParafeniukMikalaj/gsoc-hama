package org.apache.hama.examples.linearalgebra.mappers;

import org.apache.hama.examples.linearalgebra.structures.Point;

/**
 * This abstract class allows only define one-dimension mapping by extending
 * from it.
 */
public abstract class AbstractOneDimensionalMapper extends AbstractMapper {

  /**
   * {@inheritDoc}
   */
  @Override
  public int owner(int row, int column) {
    return owner(toOneDimension(new Point(row, column)));
  }

}
