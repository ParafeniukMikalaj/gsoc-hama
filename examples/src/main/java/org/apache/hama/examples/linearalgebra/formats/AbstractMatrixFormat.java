package org.apache.hama.examples.linearalgebra.formats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * Base implementation of {@link MatrixFormat }, which implements
 * {@link Writable} interface. Also contains set of useful common methods like
 * counting sparsity.
 */
public abstract class AbstractMatrixFormat implements MatrixFormat {

  protected int rows, columns;

  public AbstractMatrixFormat(){
    
  }
  
  public AbstractMatrixFormat(int rows, int columns) {
    this.rows = rows;
    this.columns = columns;
    init();
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int getRows() {
    return rows;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumns() {
    return columns;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setRows(int rows) {
    this.rows = rows;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setColumns(int columns) {
    this.columns = columns;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getSparsity() {
    return ((double) getItemsCount()) / (rows * columns);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    boolean writeSparse = 3 * getSparsity() < 1;
    out.writeBoolean(writeSparse);
    out.writeInt(getItemsCount());
    out.writeInt(getRows());
    out.writeInt(getColumns());
    if (writeSparse) {
      Iterator<MatrixCell> iterator = this.getDataIterator();
      while (iterator.hasNext()) {
        MatrixCell cell = iterator.next();
        out.writeInt(cell.getRow());
        out.writeInt(cell.getColumn());
        out.writeDouble(cell.getValue());
      }
    } else {
      for (int i = 0; i < rows; i++)
        for (int j = 0; i < columns; j++)
          out.writeDouble(getCell(i, j));
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    boolean readSparse = in.readBoolean();
    int itemsCount = in.readInt();
    int rows = in.readInt();
    int columns = in.readInt();
    setRows(rows);
    setColumns(columns);
    init();
    if (readSparse) {
      for (int i = 0; i < itemsCount; i++) {
        int cellRow = in.readInt();
        int cellColumn = in.readInt();
        double cellValue = in.readDouble();
        MatrixCell cell = new MatrixCell(cellRow, cellColumn, cellValue);
        setMatrixCell(cell);
      }
    } else {
      for (int i = 0; i < rows; i++)
        for (int j = 0; j < columns; j++) {
          double cellValue = in.readDouble();
          if (cellValue != 0) {
            MatrixCell cell = new MatrixCell(i, j, cellValue);
            setMatrixCell(cell);
          }
        }
    }
  }

}
