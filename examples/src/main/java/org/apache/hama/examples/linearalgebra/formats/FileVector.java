package org.apache.hama.examples.linearalgebra.formats;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;

/**
 * This is wrapper around {@link SequenceFile} which gives possibility to
 * iterate through {@link SequenceFile}. Used to store vectors.
 */
public class FileVector{
  private final String tmpSuffix = "/vector-files/";
  private String pathString;
  private HamaConfiguration conf;
  private Path path;
  private SequenceFile.Writer writer;
  private int dimension, count;
  
  public FileVector(){
    
  };
  
  public FileVector(int dimension) {
    this.dimension = dimension;
  };

  private class FileVectorIterator implements Iterator<VectorCell> {
    private SequenceFile.Reader reader;
    private VectorCell currentCell = new VectorCell();
    private boolean readingMade = true;

    public FileVectorIterator() throws IOException {
      this.reader = getReader();
    }

    @Override
    public boolean hasNext() {
      try {
        if (!readingMade)
          return true;
        readingMade = false;
        return reader.next(currentCell, NullWritable.get());
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }

    @Override
    public VectorCell next() {
      if (!hasNext())
        throw new NoSuchElementException("No more elements in file");
      readingMade = true;
      return currentCell;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "This format gives only write once read many access. Modification is not allowed");
    }

  }  

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public int getCount() {
    return count;
  }

  public Iterator<VectorCell> getDataIterator() throws IOException {
    return new FileVectorIterator();
  }

  private String generateTmpPathString() {
    HamaConfiguration conf = getConf();
    String prefix = conf.get("hadoop.tmp.dir", "/tmp");
    pathString = prefix + tmpSuffix + System.currentTimeMillis();
    return pathString;
  }

  private Path generateTmpPath() {
    String tmpPathString = generateTmpPathString();
    return new Path(tmpPathString);
  }

  public HamaConfiguration getConf() {
    if (conf == null)
      conf = new HamaConfiguration();
    return conf;
  }

  public void setConf(HamaConfiguration conf) {
    this.conf = conf;
  }

  public String getPathString() {
    return pathString;
  }

  public void setPathString(String pathString) {
    this.pathString = pathString;
  }

  public Path getPath() {
    if (path == null) {
      if (pathString == null)
        generateTmpPathString();
      path = new Path(pathString);
    }
    return path;
  }

  public void setPath(Path path) {
    // TODO check if it is correct
    if (path != null)
      this.pathString = path.toString();
    this.path = path;
  }

  private SequenceFile.Writer getWriter() throws IOException {
    if (writer == null) {
      HamaConfiguration conf = getConf();
      if (pathString == null)
        generateTmpPathString();
      Path path = getPath();
      writer = SequenceFile.createWriter(FileSystem.get(conf), conf, path,
          VectorCell.class, NullWritable.class, CompressionType.NONE);
    }
    return writer;
  }

  private SequenceFile.Reader getReader() throws IOException {
    HamaConfiguration conf = getConf();
    if (pathString == null)
      generateTmpPath();
    Path path = getPath();
    return new SequenceFile.Reader(FileSystem.get(conf), path, conf);
  }

  public void writeCell(VectorCell cell) throws IOException {
    SequenceFile.Writer writer = getWriter();
    writer.append(cell, NullWritable.get());
    count++;
  }
  
  public void finishWriting () throws IOException{
    getWriter().close();
  }

}
