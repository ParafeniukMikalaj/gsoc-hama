package org.apache.hama.examples.linearalgebra.mappers;

/**
 * This mapper implements best cartesian mapper for matrices. It gives good 
 * partitioning and load balance in case of random sparse matrices, 
 * matrices with density close to 1.
 */
public class CyclicDiagonalMapper extends AbstractTwoDimensionalMapper{

  /**
   * {@inheritDoc}
   * This mapper is working with processor rows and columns, thats why 
   * we are working in tow dimensions. p = m x n. p - number of processors
   * m - number of rows, n - number of columns.
   */
  private int n, m;
  
  /**
   * {@inheritDoc}
   */
  public CyclicDiagonalMapper(int rows, int columns, int peerCount) {
    init(rows, columns, peerCount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int owner(int row, int column) {
    int peerColumnIndex = (row % peerCount) % n;
    int peerRowIndex = (column % peerCount) / m;
    return peerColumnIndex + peerRowIndex * n;
  }

}
