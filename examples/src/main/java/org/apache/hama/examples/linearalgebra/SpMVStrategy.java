package org.apache.hama.examples.linearalgebra;

import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.mappers.Mapper;

public interface SpMVStrategy {

  void analyze(Matrix m, Vector v, int peerCount);

  Mapper getMatrixMapper();

  Mapper getVMapper();

  Mapper getUMapper();

  Matrix getMatrixFormat();
  
  Matrix getNewMatrixFormat();

  Vector getVectorFormat();
}
