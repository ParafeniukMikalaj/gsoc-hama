package org.apache.hama.examples.linearalgebra.formats;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * Base interface for all supported vector formats.
 */
public interface VectorFormat extends Writable {

  /**
   * Method return cell iterator.
   */
  public Iterator<VectorCell> getDataIterator();

  /**
   * Set vector cell by column and value from cell.
   */
  public void setVectorCell(VectorCell cell);

  /**
   * User should call this method after setting vector dimension. It is a good
   * place for data allocation and initialization.
   */
  public void init();

  /**
   * Returns vector's dimension.
   */
  public int getDimension();

  /**
   * Sets vector's dimension.
   */
  public void setDimension(int rows);

  /**
   * Get value of cell by position.
   */
  public double getCell(int position);

  /**
   * Check if vector contains non-zero cell in specified position.
   * 
   * @return true if vector contains cell, false otherwise.
   */
  public boolean hasCell(int position);

  /**
   * Returns number of non-zero items in vector format.
   */
  public abstract int getItemsCount();

  /**
   * Counts vector sparsity.
   */
  public double getSparsity();

}
