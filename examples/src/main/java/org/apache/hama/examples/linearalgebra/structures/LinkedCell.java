package org.apache.hama.examples.linearalgebra.structures;

/**
 * Class represends Matrix cell linked in four directions: right, left, top,
 * bottom. Used in {@link DoubleLinkeMatrix}.
 */
public class LinkedCell {

  private MatrixCell cell;
  private LinkedCell left, right, top, bottom;

  public MatrixCell getCell() {
    return cell;
  }

  public void setCell(MatrixCell cell) {
    this.cell = cell;
  }

  public LinkedCell getLeft() {
    return left;
  }

  public void setLeft(LinkedCell left) {
    this.left = left;
  }

  public LinkedCell getRight() {
    return right;
  }

  public void setRight(LinkedCell right) {
    this.right = right;
  }

  public LinkedCell getTop() {
    return top;
  }

  public void setTop(LinkedCell top) {
    this.top = top;
  }

  public LinkedCell getBottom() {
    return bottom;
  }

  public void setBottom(LinkedCell bottom) {
    this.bottom = bottom;
  }

}
