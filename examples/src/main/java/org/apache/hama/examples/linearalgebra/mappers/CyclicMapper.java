package org.apache.hama.examples.linearalgebra.mappers;

/**
 * This is cyclic mapper for vectors. It gives good partitioning 
 * and load balance for random sparse matrices and dense matrices
 * in couple with {@link CyclicDiagonalMapper}}
 */
public class CyclicMapper extends AbstractOneDimensionalMapper{

  /**
   * {@inheritDoc}
   */
  public CyclicMapper(int rows, int columns, int peerCount) {
    init(rows, columns, peerCount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int owner(int index) {
    return index % peerCount;
  }

}
