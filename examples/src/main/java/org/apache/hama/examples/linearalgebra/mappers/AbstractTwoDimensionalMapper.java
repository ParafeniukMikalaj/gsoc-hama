package org.apache.hama.examples.linearalgebra.mappers;

import org.apache.hama.examples.linearalgebra.structures.Point;

/**
 * This abstract class allows only define two-dimension mapping by extending
 * from it.
 */
public abstract class AbstractTwoDimensionalMapper extends AbstractMapper {

  /**
   * {@inheritDoc}
   */
  @Override
  public int owner(int index) {
    Point p = toTwoDimension(index);
    return owner(p.getX(), p.getY());
  }

}
