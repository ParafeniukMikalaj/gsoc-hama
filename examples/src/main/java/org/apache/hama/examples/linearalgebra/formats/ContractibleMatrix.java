package org.apache.hama.examples.linearalgebra.formats;

import java.util.HashMap;

/**
 * This interface is needed because we want to work in SpMV only with local
 * indeces. And in fanin phase we want to get result in original indeces.
 */
public interface ContractibleMatrix extends Matrix {
  /**
   * In this method matrix format should compress internal state to operate with
   * local indeces with ability to restore it original state to return result in
   * necessary format.
   */
  public void compress();

  /**
   * Returns mapping from global to local row indeces.
   */
  public HashMap<Integer, Integer> getRowMapping();

  /**
   * Returns mapping from global to local column indeces.
   */
  public HashMap<Integer, Integer> getColumnMapping();

  /**
   * Returns mapping from local to global row indeces.
   */
  public HashMap<Integer, Integer> getBackRowMapping();

  /**
   * Returns mapping from local to global column indeces.
   */
  public HashMap<Integer, Integer> getBackColumnMapping();
}
