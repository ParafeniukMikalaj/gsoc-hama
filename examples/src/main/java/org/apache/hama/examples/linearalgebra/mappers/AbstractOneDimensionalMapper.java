package org.apache.hama.examples.linearalgebra.mappers;

import org.apache.hama.examples.linearalgebra.structures.Point;

public abstract class AbstractOneDimensionalMapper extends AbstractMapper{

  @Override
  public int owner(int row, int column) {
    return owner(toOneDimension(new Point(row, column)));
  }

}
