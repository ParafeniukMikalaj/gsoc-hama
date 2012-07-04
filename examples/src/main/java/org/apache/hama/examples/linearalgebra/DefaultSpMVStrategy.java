package org.apache.hama.examples.linearalgebra;

import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.mappers.CyclicDiagonalMapper;
import org.apache.hama.examples.linearalgebra.mappers.CyclicMapper;
import org.apache.hama.examples.linearalgebra.mappers.Mapper;

/**
 * Simple implementation of {@link SpMVStrategy} Uses cartesian diagonal
 * distribution for matrix and cyclic cartesian distribution for vectors.
 * Doesn't converts matrices and vectors to other formats.
 */
public class DefaultSpMVStrategy implements SpMVStrategy {
  private Mapper mMapper, vMapper, uMapper;
  private Matrix matrixFormat;
  private Vector vectorFormat;

  /**
   * {@inheritDoc}
   */
  public void analyze(Matrix m, Vector v, int peerCount) {
    matrixFormat = m;
    vectorFormat = v;
    int rows = m.getRows();
    int columns = m.getColumns();
    mMapper = new CyclicDiagonalMapper(rows, columns, peerCount);
    vMapper = new CyclicMapper(rows, columns, peerCount);
    uMapper = new CyclicMapper(rows, columns, peerCount);
  }

  /**
   * {@inheritDoc}
   */
  public Mapper getMatrixMapper() {
    return mMapper;
  }

  /**
   * {@inheritDoc}
   */
  public Mapper getVMapper() {
    return vMapper;
  }

  /**
   * {@inheritDoc}
   */
  public Mapper getUMapper() {
    return uMapper;
  }

  /**
   * {@inheritDoc}
   */
  public Matrix getMatrixFormat() {
    return matrixFormat;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Matrix getNewMatrixFormat() {
    try {
      Matrix m = (Matrix) matrixFormat.getClass().newInstance();
      m.setRows(matrixFormat.getRows());
      m.setColumns(matrixFormat.getColumns());
      m.init();
      return m;
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public Vector getVectorFormat() {
    return vectorFormat;
  }

}
