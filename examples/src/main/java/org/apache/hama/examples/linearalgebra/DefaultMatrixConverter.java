package org.apache.hama.examples.linearalgebra;

import java.util.Iterator;

/**
 * This class implements converter for matrix formats.
 * It works with any format which extends MatrixFormat
 * and not exploits internal data structures.
 */
public class DefaultMatrixConverter<F extends MatrixFormat, T extends MatrixFormat>
    implements MatrixConverter<F, T> {
  
  /**
   * s and f must be not null to avoid code related to reflection
   */
  @Override
  public void convert(F fromFormat, T toFormat) {
    if (toFormat == null || fromFormat == null)
      throw new IllegalArgumentException(
          "ERROR in convertion. In and out Structures must be initialized");
    MatrixCell cell = null;
    int rows = fromFormat.getRows();
    int columns = fromFormat.getColumns();
    toFormat.setRows(rows);
    toFormat.setColumns(columns);
    toFormat.init();
    Iterator<MatrixCell> cellIterator = fromFormat.getDataIterator();
    while (cellIterator.hasNext()) {
      cell = cellIterator.next();
      toFormat.setMatrixCell(cell);
    }
  }

}
