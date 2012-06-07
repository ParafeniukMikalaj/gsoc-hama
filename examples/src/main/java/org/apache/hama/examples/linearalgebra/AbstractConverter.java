package org.apache.hama.examples.linearalgebra;

public abstract class AbstractConverter <F extends AbstractMatrixFormat, S extends AbstractMatrixFormat> {
  public abstract S convertForward(F f);
  public abstract F convertBack(S f);
}
