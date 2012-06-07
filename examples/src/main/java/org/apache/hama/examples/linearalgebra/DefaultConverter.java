package org.apache.hama.examples.linearalgebra;

import java.lang.reflect.ParameterizedType;
import java.util.Iterator;

public class DefaultConverter<F extends AbstractMatrixFormat, S extends AbstractMatrixFormat> 
extends AbstractConverter<F, S>{

  @Override
  public S convertForward(F f) {
    MatrixCell cell = null;
    int rows = f.getRows();
    int columns = f.getColumns();
    S resultFormat = createSecondInstance();
    resultFormat.setRows(rows);
    resultFormat.setColumns(columns);
    resultFormat.init();
    Iterator<MatrixCell> cellIterator = f.getDataIterator();
    while (cellIterator.hasNext()){
      cell = cellIterator.next();
      resultFormat.setMatrixCell(cell);
    }    
    return resultFormat;
  }

  @Override
  public F convertBack(S s) {
    MatrixCell cell = null;
    int rows = s.getRows();
    int columns = s.getColumns();
    F resultFormat = createFirstInstance();
    resultFormat.setRows(rows);
    resultFormat.setColumns(columns);
    resultFormat.init();
    Iterator<MatrixCell> cellIterator = s.getDataIterator();
    while (cellIterator.hasNext()){
      cell = cellIterator.next();
      resultFormat.setMatrixCell(cell);
    }    
    return resultFormat;
  }
  
  private F createFirstInstance(){
    try {
      F instance = (F) ((Class)((ParameterizedType)this.getClass().
          getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
      return instance;
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  private S createSecondInstance(){
    try {
      S instance = (S) ((Class)((ParameterizedType)this.getClass().
          getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
      return instance;
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return null;
  }



}
