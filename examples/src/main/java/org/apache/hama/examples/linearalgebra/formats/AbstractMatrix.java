package org.apache.hama.examples.linearalgebra.formats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * Base implementation of {@link Matrix }, which implements {@link Writable}
 * interface. Also contains set of useful common methods like counting sparsity.
 */
public abstract class AbstractMatrix implements Matrix {

  protected int rows, columns;
  protected List<Integer> nonzeroRows, nonzeroColumns;
  private boolean inited = false;

  public AbstractMatrix() {
    init();
  }

  public AbstractMatrix(int rows, int columns) {
    this.rows = rows;
    this.columns = columns;
    init();
  }
  
  /**
   * Method for basic spmv testing
   */
  public void setData(double[][] data) {
    for (int i = 0; i < data.length; i++)
      for (int j = 0; j < data[0].length; j++)
        if (data[i][j] != 0)
          setMatrixCell(new MatrixCell(i, j, data[i][j]));
  }
  
  @Override
  public void init() {
    inited = true;
    nonzeroRows = new ArrayList<Integer>();
    nonzeroColumns = new ArrayList<Integer>();   
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

  @Override
  public void setMatrixCell(MatrixCell cell) {
    int row = cell.getRow();
    int column = cell.getColumn();
    if (!nonzeroRows.contains(row))
      nonzeroRows.add(row);
    if (!nonzeroColumns.contains(column))
      nonzeroColumns.add(column);
  }

  public List<Integer> getNonZeroRows() {
    return nonzeroRows;
  }

  public List<Integer> getNonZeroColumns() {
    System.out.println("Returning columns");
    
    if  (!inited)
      init();
    if (nonzeroColumns == null)
      System.out.println("Returning null columns");
    return nonzeroColumns;
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
      while (iterator.hasNext())
        iterator.next().write(out);
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
        MatrixCell cell = new MatrixCell();
        cell.readFields(in);
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
