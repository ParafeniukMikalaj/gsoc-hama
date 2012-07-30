package org.apache.hama.examples.la;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;

/**
 * This class can generate random matrix. It uses {@link MyGenerator}. You can
 * specify different options in command line. {@link parseArgs} for more info.
 * Option for symmetric matrices is not supported yet.
 */
public class RandomMatrixGenerator {
  private static Path TMP_OUTPUT = new Path("/tmp/matrix-gen-"
      + System.currentTimeMillis());

  // private static enum totalCounter {
  // TOTAL_COUNT
  // }

  private static HamaConfiguration conf;

  public static String requestedBspTasksString = "bsptask.count";

  public static String sparsityString = "randomgenerator.sparsity";

  public static String rowsString = "randomgenerator.rows";

  public static String columnsString = "randomgenerator.columns";

  public static String outputString = "randomgenerator.output";

  private static Counter totalCounter;

  public static boolean configurationNull() {
    return conf == null;
  }

  public static void setConfiguration(HamaConfiguration configuration) {
    conf = configuration;
  }

  public static int getRequestedBspTasksCount() {
    return conf.getInt(requestedBspTasksString, -1);
  }

  public static void setRequestedBspTasksCount(int requestedBspTasksCount) {
    conf.setInt(requestedBspTasksString, requestedBspTasksCount);
  }

  public static float getSparsity() {
    return conf.getFloat(sparsityString, 0.1f);
  }

  public static void setSparsity(float sparsity) {
    conf.setFloat(sparsityString, sparsity);
  }

  public static int getRows() {
    return conf.getInt(rowsString, 10);
  }

  public static void setRows(int rows) {
    conf.setInt(rowsString, rows);
  }

  public static int getColumns() {
    return conf.getInt(columnsString, 10);
  }

  public static void setColumns(int columns) {
    conf.setInt(columnsString, columns);
  }

  public static String getOutputPath() {
    return conf.get(outputString);
  }

  public static void setOutputPath(String outputPath) {
    conf.set(outputString, outputPath);
  }

  public static int getGeneratedCount() {
    return (int) totalCounter.getValue();
  }

  /**
   * Now function parses unix-like command line. Format: -option=value Options:
   * -o - Output path for generator -r - Rows count -c - Columns count -s -
   * sparsity -n - requested bsp task number Example: Assign 5x5 matrix with 0.2
   * sparsity and request 5 bsp tasks. -r=5 -c=5 -s=0.2 -n=5
   **/
  public static void parseArgs(String[] args) {
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

        if (option.equals("-o")) {
          RandomMatrixGenerator.setOutputPath(value);
          continue;
        }

        if (option.equals("-n")) {
          try {
            int requestedBspTasksCount = Integer.parseInt(value);
            if (requestedBspTasksCount < 0)
              throw new IllegalArgumentException(
                  "The number of requested bsp tasks can't be negative. Actual value: "
                      + String.valueOf(requestedBspTasksCount));
            RandomMatrixGenerator
                .setRequestedBspTasksCount(requestedBspTasksCount);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "The format of requested bsp tasks is int. Can not parse value: "
                    + value);
          }
          continue;
        }

        if (option.equals("-r")) {
          try {
            int rows = Integer.parseInt(value);
            if (rows < 0)
              throw new IllegalArgumentException(
                  "The number of matrix rows can't be negative. Actual value: "
                      + String.valueOf(rows));
            RandomMatrixGenerator.setRows(rows);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "The format of matrix rows is int. Can not parse value: "
                    + value);
          }
          continue;
        }

        if (option.equals("-c")) {
          try {
            int columns = Integer.parseInt(value);
            if (columns < 0)
              throw new IllegalArgumentException(
                  "The number of matrix columns can't be negative. Actual value: "
                      + String.valueOf(columns));
            RandomMatrixGenerator.setColumns(columns);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "The format of matrix columns is int. Can not parse value: "
                    + value);
          }
          continue;
        }

        if (option.equals("-s")) {
          try {
            float sparsity = Float.parseFloat(value);
            if (sparsity < 0.0 || sparsity > 1.0)
              throw new IllegalArgumentException(
                  "Sparsity must be between 0.0 and 1.0. Actual value: "
                      + String.valueOf(sparsity));
            RandomMatrixGenerator.setSparsity(sparsity);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "The format of sparsity is float. Can not parse value: "
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

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    if (RandomMatrixGenerator.configurationNull())
      RandomMatrixGenerator.setConfiguration(conf);
    parseArgs(args);
    startTask();
  }

  /**
   * Alternative way to start RandomMatrixGenerator. requestedBstTasksCount and
   * outputPath parameters are optional.
   */
  public static void main(int rows, int columns, float sparsity,
      Integer requestedBspTasksCount, String outputPath) throws IOException,
      InterruptedException, ClassNotFoundException {

    HamaConfiguration conf = new HamaConfiguration();
    if (RandomMatrixGenerator.configurationNull())
      RandomMatrixGenerator.setConfiguration(conf);

    if (rows < 0)
      throw new IllegalArgumentException(
          "The number of matrix rows can't be negative. Actual value: "
              + String.valueOf(rows));

    if (columns < 0)
      throw new IllegalArgumentException(
          "The number of matrix columns can't be negative. Actual value: "
              + String.valueOf(columns));

    if (sparsity < 0.0 || sparsity > 1.0)
      throw new IllegalArgumentException(
          "Sparsity must be between 0.0 and 1.0. Actual value: "
              + String.valueOf(sparsity));

    if (requestedBspTasksCount != null && requestedBspTasksCount < 0)
      throw new IllegalArgumentException(
          "The number of requested bsp tasks can't be negative. Actual value: "
              + String.valueOf(requestedBspTasksCount));

    setRows(rows);
    setColumns(columns);
    setSparsity(sparsity);
    if (requestedBspTasksCount != null)
      setRequestedBspTasksCount(requestedBspTasksCount);
    if (outputPath != null)
      setOutputPath(outputPath);

    startTask();
  }

  private static void startTask() throws IOException, InterruptedException,
      ClassNotFoundException {
    totalCounter = new Counter() {

    };
    // conf is already not null because it is inited in main method.
    BSPJob bsp = new BSPJob(conf, RandomMatrixGenerator.class);
    bsp.setJobName("Random Matrix Generator");
    /*
     * Generator doesn't reads input. the output will be presented as matrix
     * rows with row index key. TextOutputFormat is for readability it will be
     * replaces by SequenceFileOutputFormat.
     */
    bsp.setBspClass(MyGenerator.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(SparseVectorWritable.class);
    String pathString = getOutputPath();
    Path path = TMP_OUTPUT;
    if (pathString != null)
      path = new Path(pathString);
    else
      setOutputPath(TMP_OUTPUT.toString());
    FileOutputFormat.setOutputPath(bsp, path);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (RandomMatrixGenerator.getRequestedBspTasksCount() != -1) {
      bsp.setNumBspTask(RandomMatrixGenerator.getRequestedBspTasksCount());
    } else {
      // Set to maximum
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds. Output is in " + getOutputPath().toString());
    }

  }

  /**
   * This class uses cyclic matrix distribution. Can generate random matrix. In
   * case of sparsity > 0.5 The number of generated items can be not exact. This
   * was made to achieve at least linear performance in case of high sparsity.
   */
  public static class MyGenerator
      extends
      BSP<NullWritable, NullWritable, IntWritable, SparseVectorWritable, BytesWritable> {
    public static final Log LOG = LogFactory.getLog(MyGenerator.class);

    // Some shared fields

    private static int rows, columns;
    private static float sparsity;
    private static int remainder, quotient, needed;
    private static int peerCount = 1;
    // This array is used to store final output
    private static Random rand;
    private static double criticalSparsity = 0.5;

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, IntWritable, SparseVectorWritable, BytesWritable> peer)
        throws IOException {
      sparsity = getSparsity();
      rows = getRows();
      columns = getColumns();
      int total = rows * columns;
      peerCount = peer.getNumPeers();
      rand = new Random();
      needed = (int) (total * sparsity);
      remainder = needed % rows;
      quotient = needed / rows;
    }

    /**
     * This algorithm consists of few supersteps. 1) Every peer count limit of
     * it's logical index, generates matrix items, sends number of generated
     * items to masterTask. 2) masterTasks analyzes the number of generated
     * items by each peer, counts offset for writing into result array for each
     * peer, sends offset. 3) Each peer writes it's data from received offset.
     * NOTE: in case of sparsity > 0.5 number of generated items can differ from
     * expected count.
     */
    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, IntWritable, SparseVectorWritable, BytesWritable> peer)
        throws IOException, SyncException, InterruptedException {

      List<String> peerNamesList = Arrays.asList(peer.getAllPeerNames());
      int peerIndex = peerNamesList.indexOf(peer.getPeerName());

      HashSet<Integer> createdIndeces = new HashSet<Integer>();
      List<Integer> rowIndeces = new ArrayList<Integer>();
      int tmpIndex = peerIndex;
      while (tmpIndex < rows) {
        rowIndeces.add(tmpIndex);
        tmpIndex += peerCount;
      }

      for (int rowIndex : rowIndeces) {
        SparseVectorWritable row = new SparseVectorWritable();
        row.setSize(columns);
        createdIndeces.clear();
        int needsToGenerate = quotient;
        if (rowIndex < remainder)
          needsToGenerate++;
        if (sparsity < criticalSparsity) {
          // algorithm for sparse matrices.
          while (createdIndeces.size() < needsToGenerate) {
            int index = (int) (rand.nextDouble() * columns);
            if (!createdIndeces.contains(index)) {
              totalCounter.increment(1L);
              double value = rand.nextDouble();
              row.addCell(index, value);
              createdIndeces.add(index);
            }
          }
        } else {
          // algorithm for dense matrices
          for (int i = 0; i < columns; i++)
            if (rand.nextDouble() < sparsity) {
              totalCounter.increment(1L);
              double value = rand.nextDouble();
              row.addCell(i, value);
            }
        }
        /*
         * Maybe some optimization can be performed here in case of very sparse
         * matrices with empty rows. But I am confused: how to store number of
         * non-zero rows with saving partitioning by rows in SpMV.
         */
        // if (row.getSize() > 0)
        peer.write(new IntWritable(rowIndex), row);
      }
    }
  }

}