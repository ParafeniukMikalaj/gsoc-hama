package org.apache.hama.examples.linearalgebra.mappers;

/**
 * This is basic interface which all mappers must implements.
 */
public interface Mapper {
  
  public void init(int rows, int columns, int peerCount);
  
  public int owner(int row, int column);
  
  public int owner(int index);

}
