package org.apache.hama.examples.linearalgebra.mappers;

import org.apache.hama.examples.linearalgebra.structures.Point;

public abstract class AbstractTwoDimensionalMapper extends AbstractMapper{

  @Override
  public int owner(int index) {
    Point p = toTwoDimension(index);
    return owner(p.getX(), p.getY());
  }

}
