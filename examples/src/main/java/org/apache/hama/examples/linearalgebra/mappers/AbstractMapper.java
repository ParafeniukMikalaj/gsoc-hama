package org.apache.hama.examples.linearalgebra.mappers;

import org.apache.hama.examples.linearalgebra.structures.Point;

/**
 * This class can also be used for vector distribution.
 * Just set columns to dimension and rows to 1
 */
public abstract class AbstractMapper implements Mapper{
  protected int peerCount;
  protected int rows, columns;
   
  @Override
  public void init(int rows, int columns, int peerCount){
    this.rows = rows;
    this.columns = columns;
    this.peerCount = peerCount;
  }

  protected int toOneDimension(Point point) {
    return point.getX() * columns + point.getY();
  }
  
  protected Point toTwoDimension(int index) {
    return new Point(index/columns, index % columns);
  }

  @Override
  public abstract int owner(int row, int column);
  
  @Override
  public abstract int owner(int index);

}
