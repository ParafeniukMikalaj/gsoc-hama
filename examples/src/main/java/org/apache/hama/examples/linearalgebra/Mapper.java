package org.apache.hama.examples.linearalgebra;

public interface Mapper {
  
  public abstract int toGlobal(int peerNumber, int localIndex);

  public abstract int toLocal(int peerNumber, int globalIndex);
  
}
