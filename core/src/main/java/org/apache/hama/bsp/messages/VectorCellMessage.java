package org.apache.hama.bsp.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class VectorCellMessage extends BSPMessage{

  private final String indexTag = "index";
  private final String valueTag = "value";
  private String tag;
  private int index;
  private double value;
  
  public VectorCellMessage(){
    super();
  }
  
  public VectorCellMessage(String tag, int index, double value) {
    this.tag = tag;
    this.index = index;
    this.value = value;
  }
    
  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tag = in.readUTF();
    index = in.readInt();
    value = in.readDouble();    
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tag);
    out.writeInt(index);
    out.writeDouble(value);    
  }

  @Override
  public Object getTag() {
    return tag;
  }

  @Override
  public Object getData() {
    HashMap<String, String> result = new HashMap<String, String>();
    result.put(indexTag, String.valueOf(index));
    result.put(valueTag, String.valueOf(value));
    return result;
  }

  @Override
  public void setTag(Object tag) {
    this.tag = (String) tag;    
  }

  @Override
  public void setData(Object data) {
    HashMap<String, String> map = (HashMap<String, String>)data;
    this.index = Integer.parseInt(map.get(indexTag));
    this.value = Double.parseDouble(map.get(valueTag));    
  }

}
