package org.apache.hama.examples.linearalgebra;

public interface Converter <F extends MatrixFormat, T extends MatrixFormat> {
  public void convert(F f, T t);
}
