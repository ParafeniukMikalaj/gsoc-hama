package org.apache.hama.examples.la;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;

/**
 * This class is supposed to hide some operations for converting matrices and
 * vectors from file format to in-memory format. Also contains method for
 * converting output of SpMV task to {@link DenseVectorWritable} Most of methods
 * are only needed for test purposes.
 */
public class WritableUtil {

  /**
   * This method gives the ability to write matrix from memory to file. It
   * should be used with small matrices and for test purposes only.
   * 
   * @param pathString
   *          path to file where matrix will be writed.
   * @param conf
   *          configuration
   * @param matrix
   *          map of row indeces and values presented as {@link Writable}
   * @throws IOException
   */
  public void writeMatrix(String pathString, Configuration conf,
      Map<Integer, Writable> matrix) throws IOException {
    boolean inited = false;
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer writer = null;
    try {
      for (Integer index : matrix.keySet()) {
        IntWritable key = new IntWritable(index);
        Writable value = matrix.get(index);
        if (!inited) {
          writer = new SequenceFile.Writer(fs, conf, new Path(pathString),
              IntWritable.class, value.getClass());
          inited = true;
        }
        writer.append(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null)
        writer.close();
    }

  }

  /**
   * This method is used to read vector from specified path in {@link SpMVTest}.
   * For test purposes only.
   * 
   * @param pathString
   *          input path for vector
   * @param result
   *          instanse of vector writable which should be filled.
   * @param conf
   *          configuration
   * @throws IOException
   */
  public void readFromFile(String pathString, Writable result,
      Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, new Path(pathString), conf);
      IntWritable key = new IntWritable();
      reader.next(key, result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * This method is used to write vector from memory to specified path.
   * 
   * @param pathString
   *          output path
   * @param result
   *          instance of vector to be writed
   * @param conf
   *          configuration
   * @throws IOException
   */
  public void writeToFile(String pathString, Writable result, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer writer = null;
    try {
      writer = new SequenceFile.Writer(fs, conf, new Path(pathString),
          IntWritable.class, result.getClass());
      IntWritable key = new IntWritable();
      writer.append(key, result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null)
        writer.close();
    }
  }

  /**
   * SpMV produces a file, which contains result dense vector in format of pairs
   * of integer and double. The aim of this method is to convert SpMV output to
   * format usable in subsequent computation - dense vector. It can be usable
   * for iterative solvers. IMPORTANT: currently it is used in {@link SpMV}. It
   * can be a bottle neck, because all input needs to be stored in memory.
   * 
   * @param SpMVoutputPathString
   *          output path, which represents directory with part files.
   * @param conf
   *          configuration
   * @param size
   *          size of generated result vector. retrieved from counter.
   * @return path to output vector.
   * @throws IOException
   */
  public String convertSpMVOutputToDenseVector(String SpMVoutputPathString,
      Configuration conf, int size) throws IOException {
    DenseVectorWritable result = new DenseVectorWritable();
    result.setSize(size);
    FileSystem fs = FileSystem.get(conf);
    Path SpMVOutputPath = new Path(SpMVoutputPathString);
    Path resultOutputPath = SpMVOutputPath.getParent().suffix("/result");
    FileStatus[] stats = fs.listStatus(SpMVOutputPath);
    for (FileStatus stat : stats) {
      String filePath = stat.getPath().toUri().getPath();
      SequenceFile.Reader reader = null;
      fs.open(new Path(filePath));
      try {
        reader = new SequenceFile.Reader(fs, new Path(filePath), conf);
        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        while (reader.next(key, value))
          result.addCell(key.get(), value.get());
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (reader != null)
          reader.close();
      }
    }
    writeToFile(resultOutputPath.toString(), result, conf);
    return resultOutputPath.toString();
  }
}
