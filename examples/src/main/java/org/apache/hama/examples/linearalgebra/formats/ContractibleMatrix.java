package org.apache.hama.examples.linearalgebra.formats;

import java.util.HashMap;

/**
 * This interface is needed because we want to work in SpMV
 * only with local indeces. And in fanin phase we want to 
 * get result in original indeces.
 * @author mikalaj
 *
 */
public interface ContractibleMatrix extends Matrix{
  /**
   * In this method matrix format should compress internal 
   * state to operate with local indeces with ability to
   * restore it original state to return result in necessary format.
   */
  public void compress();
  
  public HashMap<Integer, Integer> getRowMapping();
  
  public HashMap<Integer, Integer> getColumnMapping();
  
  public HashMap<Integer, Integer> getBackRowMapping();
  
  public HashMap<Integer, Integer> getBackColumnMapping();
}
