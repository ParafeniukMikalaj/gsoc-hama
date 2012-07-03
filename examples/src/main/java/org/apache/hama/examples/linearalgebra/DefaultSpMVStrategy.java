package org.apache.hama.examples.linearalgebra;

import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.mappers.CyclicDiagonalMapper;
import org.apache.hama.examples.linearalgebra.mappers.CyclicMapper;
import org.apache.hama.examples.linearalgebra.mappers.Mapper;

public class DefaultSpMVStrategy implements SpMVStrategy{
  private Mapper matrixMapper, vMapper, uMapper;
  private Matrix matrixFormat;
  private Vector vectorFormat;
  public void analyze(Matrix m, Vector v, int peerCount) {
    matrixFormat = m;
    vectorFormat = v;
    int rows = m.getRows();
    int columns = m.getColumns();
    matrixMapper = new CyclicDiagonalMapper(rows, columns, peerCount);
    vMapper = new CyclicMapper(rows, columns, peerCount);
    uMapper = new CyclicMapper(rows, columns, peerCount);
  }

  public Mapper getMatrixMapper() {
    return matrixMapper;
  }
  
  public Mapper getVMapper() {
    return vMapper;
  }
  
  public Mapper getUMapper() {
    return uMapper;
  }

  public Matrix getMatrixFormat() {
    return matrixFormat;
  }
  
  @Override
  public Matrix getNewMatrixFormat() {
    try {
      Matrix m = (Matrix)matrixFormat.getClass().newInstance();
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

  public Vector getVectorFormat() {
    return vectorFormat;
  }


}
