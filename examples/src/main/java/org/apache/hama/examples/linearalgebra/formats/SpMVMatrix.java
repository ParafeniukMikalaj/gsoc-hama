package org.apache.hama.examples.linearalgebra.formats;

import java.util.List;

public interface SpMVMatrix {
  public List<Integer> getNonZeroRows();
  public List<Integer> getNonZeroColumns();
}
