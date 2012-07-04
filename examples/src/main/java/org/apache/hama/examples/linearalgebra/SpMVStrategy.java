package org.apache.hama.examples.linearalgebra;

import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.mappers.Mapper;

/**
 * This is interface for strategy for Sparse Matrix Vector Multiplication
 * problem. It can choose appropriate matrix and vector formats, and choose
 * appropriate mappers.
 */
public interface SpMVStrategy {

  /**
   * This method should be called before usage of other methods of class. It
   * analyze input and initialize internal data.
   */
  void analyze(Matrix m, Vector v, int peerCount);

  /**
   * Returns {@link Mapper} for matrix m.
   */
  Mapper getMatrixMapper();

  /**
   * Returns {@link Mapper} for input vector v.
   */
  Mapper getVMapper();

  /**
   * Returns {@link Mapper} for vector of partial sum u.
   */
  Mapper getUMapper();

  /**
   * Returns appropriate matrix format.
   */
  Matrix getMatrixFormat();

  /**
   * Returns new instance of appropriate matrix format.
   */
  Matrix getNewMatrixFormat();

  /**
   * Returns appropriate vector format.
   */
  Vector getVectorFormat();
}
