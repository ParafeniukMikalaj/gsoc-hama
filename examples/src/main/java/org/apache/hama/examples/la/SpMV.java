package org.apache.hama.examples.la;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.messages.ByteMessage;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public class SpMV {

  private static HamaConfiguration conf;
  private static final String outputPathString = "spmv.outputpath";
  private static final String inputMatrixPathString = "spmv.inputmatrixpath";
  private static final String inputVectorPathString = "spmv.inputvectorpath";
  private static String requestedBspTasksString = "bsptask.count";
  private static final String spmvSuffix = "/spmv/";

  public static HamaConfiguration getConfiguration() {
    return conf;
  }

  public static void setConfiguration(HamaConfiguration configuration) {
    conf = configuration;
  }

  public static String getOutputPath() {
    return conf.get(outputPathString, null);
  }

  public static void setOutputPath(String outputPath) {
    conf.set(outputPathString, outputPath);
  }

  public static String getInputMatrixPath() {
    return conf.get(inputMatrixPathString, null);
  }

  public static void setInputMatrixPath(String inputPath) {
    conf.set(inputMatrixPathString, inputPath);
  }

  public static String getInputVectorPath() {
    return conf.get(inputVectorPathString, null);
  }

  public static void setInputVectorPath(String inputPath) {
    conf.set(inputVectorPathString, inputPath);
  }

  public static int getRequestedBspTasksCount() {
    return conf.getInt(requestedBspTasksString, -1);
  }

  public static void setRequestedBspTasksCount(int requestedBspTasksCount) {
    conf.setInt(requestedBspTasksString, requestedBspTasksCount);
  }

  private static String generateOutPath() {
    HamaConfiguration conf = SpMV.conf;
    if (conf == null)
      conf = new HamaConfiguration();
    String prefix = conf.get("hadoop.tmp.dir", "/tmp");
    String pathString = prefix + spmvSuffix + System.currentTimeMillis();
    return pathString;
  }

  /**
   * Function parses Unix-like command line which consists of -option=value
   * pairs. Possible options: -im : path for input file matrix; -iv : path for
   * input file vector; -o : optional path for output file for dense vector; -n
   * : optional requested number of bsp peers.
   * 
   * @param args
   */
  private static void parseArgs(String[] args) {
    try {
      String[] arr;
      String option, value;
      for (String arg : args) {
        arr = arg.split("=");
        try {
          option = arr[0];
          value = arr[1];
        } catch (IndexOutOfBoundsException e) {
          throw new IllegalArgumentException(
              "Mallformed option. Usage: -option=value. Current value: " + arg);
        }
        if (option.equals("-im")) {
          SpMV.setInputMatrixPath(value);
          continue;
        }

        if (option.equals("-iv")) {
          SpMV.setInputVectorPath(value);
          continue;
        }

        if (option.equals("-o")) {
          SpMV.setOutputPath(value);
          continue;
        }

        if (option.equals("-n")) {
          try {
            int taskCount = Integer.parseInt(value);
            if (taskCount < 0)
              throw new IllegalArgumentException(
                  "The number of requested tasks can't be negative. Actual value: "
                      + String.valueOf(taskCount));
            SpMV.setRequestedBspTasksCount(taskCount);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "The format of requested task count is int. Can not parse value: "
                    + value);
          }
          continue;
        }

        throw new IllegalArgumentException("Unknown option: " + option + value);

      }
    } catch (Exception e) {
      StringBuilder st = new StringBuilder();
      for (String arg : args)
        st.append(" " + arg);
      throw new IllegalArgumentException(
          "Unexpected error in command line. cmd: " + st.toString()
              + ". Message: " + e.getMessage());
    }
  }

  /**
   * {@code output} and {@code requestedBspTaskCount} parameters can be null.
   * {@code output} can be generated and {@code requestedBspTaskCount} will be
   * setted to maximum.
   */
  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    parseArgs(args);
    startTask();
  }

  /**
   * {@code output} and {@code requestedBspTaskCount} parameters can be null.
   * {@code output} can be generated and {@code requestedBspTaskCount} will be
   * setted to maximum.
   */
  public static void main(Path inputMatrix, Path inputVector, Path output,
      Integer requestedBspTaskCount) throws IOException, InterruptedException,
      ClassNotFoundException {
    if (inputMatrix == null)
      throw new IllegalArgumentException(
          "Input path for SpMV matrix can't be null");
    if (inputVector == null)
      throw new IllegalArgumentException(
          "Input path for SpMV vector can't be null");
    if (requestedBspTaskCount != null && requestedBspTaskCount < 1)
      throw new IllegalArgumentException(
          "Number of requested bsp tasks is incorrect. It must be above zero. Actual value is "
              + requestedBspTaskCount);
    setInputMatrixPath(inputMatrix.toString());
    setInputVectorPath(inputVector.toString());
    setOutputPath(output.toString());
    setRequestedBspTasksCount(requestedBspTaskCount);
    startTask();
  }

  private static void startTask() throws IOException, InterruptedException,
      ClassNotFoundException {
    if (getConfiguration() == null)
      setConfiguration(new HamaConfiguration());
    BSPJob bsp = new BSPJob(conf, SpMV.class);
    bsp.setJobName("Sparse matrix vector multiplication");
    bsp.setBspClass(SpMVExecutor.class);
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setInputPath(new Path(getInputMatrixPath()));

    if (getOutputPath() == null)
      setOutputPath(generateOutPath());
    // FIXME check this logic.
    FileOutputFormat.setOutputPath(bsp, new Path(getOutputPath()));

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (getRequestedBspTasksCount() != -1) {
      bsp.setNumBspTask(getRequestedBspTasksCount());
    } else {
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    } else {
      setOutputPath(null);
    }
  }

  /**
   * This class performs sparse matrix vector multiplication. u = m * v. m -
   * input matrix, u - partial sum, v - input vector.
   */
  private static class SpMVExecutor
      extends
      BSP<IntWritable, MatrixRowWritable, IntWritable, DoubleWritable, ByteMessage> {
    private MatrixRowWritable v;
   

    @Override
    public void setup(
        BSPPeer<IntWritable, MatrixRowWritable, IntWritable, DoubleWritable, ByteMessage> peer)
        throws IOException, SyncException, InterruptedException {
      //reading input vector, which represented as matrix row
      FileSystem fs = FileSystem.get(peer.getConfiguration());
      SequenceFile.Reader reader = null;
      Path inputVector = new Path(getInputVectorPath());
      reader = new SequenceFile.Reader(fs, inputVector, peer.getConfiguration());
      MatrixRowWritable value = new MatrixRowWritable();
      //we are not interested in row index. it doesn't has meaning for vector.
      NullWritable key = NullWritable.get();
      reader.next(key, value);
      v = value;
    }

    @Override
    public void bsp(
        BSPPeer<IntWritable, MatrixRowWritable, IntWritable, DoubleWritable, ByteMessage> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<IntWritable, MatrixRowWritable> row = null;
      List<Double> vValues = v.getValues();
      while ((row = peer.readNext()) != null) {
        int key = row.getKey().get();
        int sum = 0;
        MatrixRowWritable matrixRowWritable = row.getValue();
        List<Integer> mIndeces = matrixRowWritable.getIndeces();
        List<Double> mValues = matrixRowWritable.getValues();
        for(int i = 0; i < mIndeces.size(); i++)
          sum+= vValues.get(mIndeces.get(i))*mValues.get(i);
        peer.write(new IntWritable(key), new DoubleWritable(sum));
      }

    }

  }
}
