package org.apache.hama.examples.la;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public interface VectorWritable extends Writable {
  public Iterator<VectorCell> getIterator();
  public void addCell(int index, double value);
}
