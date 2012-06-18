package org.apache.hama.examples.linearalgebra;

/**
 * This is basic interface which all mappers must implements.
 */
public interface Mapper {

  /**
   * This method converts local processor index to global matrix index.
   */
  public abstract int toGlobal(int peerNumber, int localIndex);

  /**
   * This method converts global matrix index to processor internal index.
   */
  public abstract int toLocal(int peerNumber, int globalIndex);

}
