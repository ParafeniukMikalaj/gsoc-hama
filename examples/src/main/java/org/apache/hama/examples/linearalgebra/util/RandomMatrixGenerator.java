package org.apache.hama.examples.linearalgebra.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.messages.IntegerMessage;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.PiEstimator.MyEstimator;
import org.apache.hama.examples.linearalgebra.formats.CRSMatrix;
import org.apache.hama.examples.linearalgebra.formats.DenseMatrix;
import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;

/**
 * This class can generate random matrix. It uses {@link MyGenerator}. You can
 * specify different options in command line. {@link parseArgs} for more info.
 * Option for symmetric matrices is not supported yet.
 */
public class RandomMatrixGenerator {
  private static Path TMP_OUTPUT = new Path("/tmp/matrix-gen-"
      + System.currentTimeMillis());

  private static HamaConfiguration conf;

  public static String requestedBspTasksString = "bsptask.count";

  public static String sparsityString = "randomgenerator.sparsity";

  public static String rowsString = "randomgenerator.rows";

  public static String columnsString = "randomgenerator.columns";

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

  /**
   * Now function parses unix-like command line. Format: -option=value Options:
   * -r - Rows count -c - Columns count -s - sparsity -n - requested bsp task
   * number Example: Assign 5x5 matrix with 0.2 sparsity and request 5 bsp
   * tasks. -r=5 -c=5 -s=0.2 -n=5
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

  /**
   * This class uses cyclic matrix distribution. Can generate random matrix. In
   * case of sparsity > 0.5 The number of generated items can be not exact. This
   * was made to achieve at least linear performance in case of high sparsity.
   */
  public static class MyGenerator
      extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, IntegerMessage> {
    public static final Log LOG = LogFactory.getLog(MyEstimator.class);

    // Some shared fields

    private static Matrix result;
    private static float neededSparsity;
    private static int remainder, quotient, neededItemsCount;
    private static int peerCount = 1;
    // This array is used to store final output
    private static Random rand;

    private Matrix createresult() {
      Matrix result;
      if (neededSparsity < 0.5)
        result = new CRSMatrix();
      else
        result = new DenseMatrix();
      return result;
    }

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, IntegerMessage> peer)
        throws IOException {
      neededSparsity = getSparsity();
      int rows = getRows();
      int columns = getColumns();
      result = createresult();
      result.setRows(rows);
      result.setColumns(columns);
      result.init();
      int totalNumber = rows * columns;
      peerCount = peer.getNumPeers();
      rand = new Random();
      remainder = totalNumber % peerCount;
      quotient = totalNumber / peerCount;
      neededItemsCount = (int) (totalNumber * neededSparsity);
    }

    /**
     * Converts logical one-dimension index to local peer index.
     */
    public int toGlobal(int localIndex, int peerNumber) {
      int offset = 0;
      if (peerNumber < remainder)
        offset = (quotient + 1) * peerNumber;
      else
        offset = (quotient + 1) * remainder + quotient
            * (peerNumber - remainder);
      return localIndex + offset;
    }

    /**
     * Converts local for peer one-dimension index to logical index.
     */
    public int toLocal(int globalIndex, int peerNumber) {
      return globalIndex / peerCount;
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
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, IntegerMessage> peer)
        throws IOException, SyncException, InterruptedException {
      int rows = result.getRows();
      int columns = result.getColumns();
      Matrix localMatrix = createresult();
      List<String> peerNamesList = Arrays.asList(peer.getAllPeerNames());
      int localIndex, logicalIndex, row, column, limit;
      double value;
      // Superstep 1
      int peerNumber = peerNamesList.indexOf(peer.getPeerName());
      // Counting limit in local indexes.
      limit = quotient;
      if (peerNumber < remainder)
        limit++;
      localMatrix.setColumns(limit);
      localMatrix.setRows(1);
      localMatrix.init();
      // Generation of matrix items
      if (neededSparsity < 0.5) {
        // Algorithm for sparse matrices
        int needsToGenerate = neededItemsCount / peerCount;
        if (peerNumber < neededItemsCount % peerCount)
          needsToGenerate++;
        while (localMatrix.getItemsCount() < needsToGenerate) {
          localIndex = (int) (rand.nextDouble() * limit);
          logicalIndex = toGlobal(localIndex, peerNumber);
          if (!localMatrix.hasCell(0, logicalIndex)) {
            row = 0;
            column = localIndex;
            value = rand.nextDouble();
            MatrixCell cell = new MatrixCell(row, column, value);
            localMatrix.setMatrixCell(cell);
          }
        }
      } else {
        // Algorithm for dense matrices
        for (int i = 0; i < limit; i++)
          if (rand.nextDouble() < neededSparsity) {
            row = 0;
            column = i;
            value = rand.nextDouble();
            MatrixCell cell = new MatrixCell(row, column, value);
            localMatrix.setMatrixCell(cell);
          }
      }
      peer.sync();
      Iterator<MatrixCell> cellIterator = localMatrix.getDataIterator();
      while (cellIterator.hasNext()) {
        MatrixCell localCell = cellIterator.next();
        localIndex = localCell.getColumn();
        logicalIndex = toGlobal(localIndex, peerNumber);
        row = logicalIndex / columns;
        column = logicalIndex % columns;
        MatrixCell logicalCell = new MatrixCell(row, column,
            localCell.getValue());

        result.setMatrixCell(logicalCell);
      }
    }

    public static Matrix getResult() {
      return result;
    }

  }

  public static Matrix getResult() {
    return MyGenerator.getResult();
  }

  /**
   * This method copies output from System.out to tmp file from configuration
   */
  static void printOutput(HamaConfiguration conf) throws IOException {
    // TODO print some additional info
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(TMP_OUTPUT);
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() > 0) {
        FSDataOutputStream out = fs.create(files[i].getPath());
        Matrix result = MyGenerator.getResult();
        result.write(out);
        break;
      }
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, RandomMatrixGenerator.class);
    // Set the job name
    bsp.setJobName("Random Matrix Generator");
    bsp.setBspClass(MyGenerator.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);
    if (RandomMatrixGenerator.configurationNull())
      RandomMatrixGenerator.setConfiguration(conf);
    parseArgs(args);

    if (RandomMatrixGenerator.getRequestedBspTasksCount() != -1) {
      bsp.setNumBspTask(RandomMatrixGenerator.getRequestedBspTasksCount());
    } else {
      // Set to maximum
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      printOutput(conf);
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}