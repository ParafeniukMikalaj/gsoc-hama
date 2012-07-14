package org.apache.hama.examples.linearalgebra.formats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * Base implementation of Vector format, which implements {@link Writable}
 * interface and adds some userful features like counting vector sparsity.
 */
public abstract class AbstractVector implements Vector {

  protected int dimension;

  public AbstractVector() {
  }

  public AbstractVector(int dimension) {
    this.dimension = dimension;
    init();
  }
  
  public void setData(double[] data) {
    for (int i = 0; i < data.length; i++){
      if (data[i] != 0)
       setVectorCell(new VectorCell(i, data[i]));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getDimension() {
    return dimension;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getSparsity() {
    return ((double) getItemsCount()) / dimension;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    boolean writeSparse = 3 * getSparsity() < 1;
    out.writeBoolean(writeSparse);
    out.writeInt(getItemsCount());
    out.writeInt(getDimension());
    if (writeSparse) {
      Iterator<VectorCell> iterator = this.getDataIterator();
      while (iterator.hasNext()) {
        VectorCell cell = iterator.next();
        out.writeInt(cell.getIndex());
        out.writeDouble(cell.getValue());
      }
    } else {
      for (int i = 0; i < getDimension(); i++)
        out.writeDouble(getCell(i));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    boolean readSparse = in.readBoolean();
    int itemsCount = in.readInt();
    int dimension = in.readInt();
    setDimension(dimension);
    init();
    if (readSparse) {
      for (int i = 0; i < itemsCount; i++) {
        int cellPosition = in.readInt();
        double cellValue = in.readDouble();
        VectorCell cell = new VectorCell(cellPosition, cellValue);
        setVectorCell(cell);
      }
    } else {
      for (int i = 0; i < dimension; i++) {
        double cellValue = in.readDouble();
        if (cellValue != 0) {
          VectorCell cell = new VectorCell(i, cellValue);
          setVectorCell(cell);
        }
      }
    }
  }

}
