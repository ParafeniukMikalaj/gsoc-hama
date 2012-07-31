package org.apache.hama.examples.la;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.messages.ByteMessage;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

/**
 * Sparse matrix vector multiplication. Currently it uses row-wise access.
 * Assumptions: 1) each peer should have copy of input vector for efficient
 * operations. 2) row-wise implementation is good because we don't need to care
 * about communication 3) the main way to improve performance - create custom
 * Partitioner
 */
public class SpMV {

  private static HamaConfiguration conf;
  private static final String outputPathString = "spmv.outputpath";
  private static final String resultPathString = "spmv.resultpath";
  private static final String inputMatrixPathString = "spmv.inputmatrixpath";
  private static final String inputVectorPathString = "spmv.inputvectorpath";
  private static String requestedBspTasksString = "bsptask.count";
  private static final String spmvSuffix = "/spmv/";
  private static final String intermediate = "/part";

  private static Counter rowCounter;

  public static HamaConfiguration getConf() {
    if (conf == null)
      conf = new HamaConfiguration();
    return conf;
  }

  public static void setConfiguration(HamaConfiguration configuration) {
    conf = configuration;
  }

  public static String getOutputPath() {
    return getConf().get(outputPathString, null);
  }

  public static void setOutputPath(String outputPath) {
    Path path = new Path(outputPath);
    path = path.suffix(intermediate);
    getConf().set(outputPathString, path.toString());
  }

  public static String getResultPath() {
    return getConf().get(resultPathString, null);
  }

  private static void setResultPath(String resultPath) {
    getConf().set(resultPathString, resultPath);
  }

  public static String getInputMatrixPath() {
    return getConf().get(inputMatrixPathString, null);
  }

  public static void setInputMatrixPath(String inputPath) {
    getConf().set(inputMatrixPathString, inputPath);
  }

  public static String getInputVectorPath() {
    return getConf().get(inputVectorPathString, null);
  }

  public static void setInputVectorPath(String inputPath) {
    getConf().set(inputVectorPathString, inputPath);
  }

  public static int getRequestedBspTasksCount() {
    return getConf().getInt(requestedBspTasksString, -1);
  }

  public static void setRequestedBspTasksCount(int requestedBspTasksCount) {
    getConf().setInt(requestedBspTasksString, requestedBspTasksCount);
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
   * This method gives opportunity to start SpMV with command line.
   */
  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    parseArgs(args);
    startTask();
  }

  /**
   * Alternative way to start SpMV task. {@code output} and {@code output} and
   * {@code requestedBspTaskCount} parameters can be null. {@code output} can be
   * generated and {@code requestedBspTaskCount} will be setted to maximum.
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

  /**
   * Method which actually starts SpMV.
   */
  private static void startTask() throws IOException, InterruptedException,
      ClassNotFoundException {
    if (getConf() == null)
      setConfiguration(new HamaConfiguration());
    rowCounter = new Counter() {

    };
    BSPJob bsp = new BSPJob(conf, SpMV.class);
    bsp.setJobName("Sparse matrix vector multiplication");
    bsp.setBspClass(SpMVExecutor.class);
    /*
     * Input matrix is presented as pairs of integer and {@ link
     * SparseVectorWritable}. Output is pairs of integer and double
     */
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
          + " seconds.");
      convertToDenseVector();
      System.out.println("Result is in " + getResultPath());
    } else {
      setResultPath(null);
    }
  }

  /**
   * IMPORTANT: This can be a bottle neck. Problem can be here{@core
   * WritableUtil.convertSpMVOutputToDenseVector()}
   */
  private static void convertToDenseVector() throws IOException {
    WritableUtil util = new WritableUtil();
    int size = (int) rowCounter.getValue();
    String resultPath = util.convertSpMVOutputToDenseVector(getOutputPath(),
        getConf(), size);
    setResultPath(resultPath);
  }

  /**
   * This class performs sparse matrix vector multiplication. u = m * v.
   */
  private static class SpMVExecutor
      extends
      BSP<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, ByteMessage> {
    private DenseVectorWritable v;

    /**
     * Each peer reads input dense vector.
     */
    @Override
    public void setup(
        BSPPeer<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, ByteMessage> peer)
        throws IOException, SyncException, InterruptedException {
      // reading input vector, which represented as matrix row
      WritableUtil util = new WritableUtil();
      v = new DenseVectorWritable();
      util.readFromFile(getInputVectorPath(), v, conf);
    }

    /**
     * Local inner product computation and output.
     */
    @Override
    public void bsp(
        BSPPeer<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, ByteMessage> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<IntWritable, SparseVectorWritable> row = null;
      while ((row = peer.readNext()) != null) {
        // it will be needed in conversion of output to result vector
        rowCounter.increment(1L);
        int key = row.getKey().get();
        int sum = 0;
        SparseVectorWritable mRow = row.getValue();
        if (v.getSize() != mRow.getSize())
          throw new RuntimeException("Matrix row with index = " + key
              + " is not consistent with input vector. Row size = "
              + mRow.getSize() + " vector size = " + v.getSize());
        List<Integer> mIndeces = mRow.getIndeces();
        List<Double> mValues = mRow.getValues();
        for (int i = 0; i < mIndeces.size(); i++)
          sum += v.get(mIndeces.get(i)) * mValues.get(i);
        peer.write(new IntWritable(key), new DoubleWritable(sum));
      }
      peer.sync();
    }

  }
}
