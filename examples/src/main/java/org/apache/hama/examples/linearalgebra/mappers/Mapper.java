package org.apache.hama.examples.linearalgebra.mappers;

/**
 * This is basic interface which all mappers must implements.
 */
public interface Mapper {

  /**
   * Initialization of Mapper.
   */
  public void init(int rows, int columns, int peerCount);

  /**
   * Get owner from two-dimensional index.
   */
  public int owner(int row, int column);

  /**
   * Get owner from one-dimensional index.
   */
  public int owner(int index);

}
