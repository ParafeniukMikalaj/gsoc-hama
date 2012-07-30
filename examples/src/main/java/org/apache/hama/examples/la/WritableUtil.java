package org.apache.hama.examples.la;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

public class WritableUtil {

  // for test purposes only.
  public void writeMatrix(String pathString, Configuration conf,
      Map<Integer, VectorWritable> matrix) throws IOException {
    boolean inited = false;
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer writer = null;
    try {
      for (Integer index : matrix.keySet()) {
        IntWritable key = new IntWritable(index);
        VectorWritable value = matrix.get(index);
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

  public VectorWritable readFromFile(String pathString, VectorWritable result,
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
    return result;
  }

  public void writeToFile(String pathString, VectorWritable result,
      Configuration conf) throws IOException {
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
