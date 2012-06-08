package org.apache.hama.examples.linearalgebra;

import java.util.Iterator;

public class DefaultConverter<F extends MatrixFormat, T extends MatrixFormat>
    implements Converter<F, T> {
 
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
