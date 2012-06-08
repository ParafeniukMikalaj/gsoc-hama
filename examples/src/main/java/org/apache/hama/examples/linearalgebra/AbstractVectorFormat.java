package org.apache.hama.examples.linearalgebra;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public abstract class AbstractVectorFormat implements VectorFormat{

  private int dimension;
  
  @Override
  public abstract Iterator<VectorCell> getDataIterator();

  @Override
  public abstract void setVectorCell(VectorCell cell);

  @Override
  public abstract void init();

  @Override
  public int getDimension() {
    return dimension;
  }

  @Override
  public void setDimension(int dimension) {
    this.dimension = dimension;    
  }

  @Override
  public abstract double getCell(int position);

  @Override
  public abstract boolean hasCell(int position);

  @Override
  public abstract int getItemsCount();

  @Override
  public double getSparsity() {
    return ((double)getItemsCount())/dimension;
  }

  @Override
  public void writeVector(DataOutput out) throws IOException {
    boolean writeSparse = 3 * getSparsity() < 1;
    out.writeBoolean(writeSparse);
    out.writeInt(getItemsCount());
    out.writeInt(getDimension());
    if (writeSparse) {
      Iterator<VectorCell> iterator = this.getDataIterator();
      while (iterator.hasNext()) {
        VectorCell cell = iterator.next();
        out.writeInt(cell.getPosition());
        out.writeDouble(cell.getValue());
      }
    } else {
      for (int i = 0; i < getDimension(); i++)
        out.writeDouble(getCell(i));
    }    
  }

  @Override
  public void readVector(DataInput in) throws IOException {
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
