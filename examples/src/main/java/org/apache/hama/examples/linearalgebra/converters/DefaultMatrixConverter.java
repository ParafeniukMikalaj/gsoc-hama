package org.apache.hama.examples.linearalgebra.converters;

import java.util.Iterator;

import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * This class implements converter for matrix formats. It works with any format
 * which extends MatrixFormat and not exploits internal data structures. It only
 * uses iterator from first format to fill second format.
 */
public class DefaultMatrixConverter<F extends Matrix, T extends Matrix>
    implements MatrixConverter<F, T> {

  /**
   * {@inheritdoc} s and f must be not null to avoid code related to reflection
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
