package org.apache.hama.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
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
import org.apache.mahout.math.map.OpenIntDoubleHashMap;
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

  public static String sparsityString = "matrix.sparsity";

  public static String rowsString = "matrix.rows";

  public static String columnsString = "matrix.columns";

  private static int generatedCount = -1;

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
        }

        if (option.equals("-c")) {
          try {
            int columns = Integer.parseInt(value);
            if (columns < 0)
              throw new IllegalArgumentException(
                  "The number of matrix columns can't be negative. Actual value: "
                      + String.valueOf(columns));
            RandomMatrixGenerator.setRows(columns);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "The format of matrix columns is int. Can not parse value: "
                    + value);
          }
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
        }

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
  public static class MyGenerator extends
      BSP<NullWritable, NullWritable, Text, DoubleWritable, IntegerMessage> {
    public static final Log LOG = LogFactory.getLog(MyEstimator.class);

    // String which identifies master task
    private String masterTask;
    // Some shared fields
    private static int rows, columns;
    private float sparsity;
    private static int remainder, quotient, itemsCount = 0;
    private static int peerCount = 1;
    // This array is used to store final output
    private static double[] result;
    private static Random rand;

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, IntegerMessage> peer)
        throws IOException {
      rows = getRows();
      columns = getColumns();
      sparsity = getSparsity();
      this.masterTask = peer.getPeerName(peer.getNumPeers() / 2);
      int totalNumber = rows * columns;
      peerCount = peer.getNumPeers();
      rand = new Random();
      remainder = totalNumber % peerCount;
      quotient = totalNumber / peerCount;
      itemsCount = (int) (totalNumber * sparsity);
    }

    /**
     * Converts logical one-dimension index to local peer index.
     */
    public static int map(int logicalNumber, int peerNumber, int peerCount) {
      return logicalNumber / peerCount;
    }

    /**
     * Converts local for peer one-dimension index to logical index.
     */
    public static int unmap(int privateNumber, int peerNumber, int peerCount) {
      return privateNumber * peerCount + peerNumber;
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
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, IntegerMessage> peer)
        throws IOException, SyncException, InterruptedException {
      ArrayList<Double> localMatrix = new ArrayList<Double>();
      List<String> peerNamesList = Arrays.asList(peer.getAllPeerNames());
      int localIndex, logicalIndex, row, column, limit;
      double value;
      // Superstep 1
      int peerNumber = peerNamesList.indexOf(peer.getPeerName());
      // Counting limit in local indexes.
      limit = quotient;
      if (peerNumber < remainder)
        limit++;
      // Generation of matrix items
      if (sparsity < 0.5) {
        // Algorithm for sparse matrices
        OpenIntDoubleHashMap indexes = new OpenIntDoubleHashMap();
        int needsToGenerate = itemsCount / peerCount;
        if (peerNumber < itemsCount % peerCount)
          needsToGenerate++;
        while (indexes.size() < needsToGenerate) {
          localIndex = (int) (rand.nextDouble() * limit);
          logicalIndex = unmap(localIndex, peerNumber, peerCount);
          if (!indexes.containsKey(logicalIndex)) {
            row = logicalIndex / rows;
            column = logicalIndex % columns;
            value = rand.nextDouble();
            localMatrix.add((double) row);
            localMatrix.add((double) column);
            localMatrix.add(value);
            indexes.put(logicalIndex, value);
          }
        }
      } else {
        // Algorithm for dense matrices
        for (int i = 0; i < limit; i++)
          if (rand.nextDouble() < sparsity) {
            localIndex = i;
            logicalIndex = unmap(localIndex, peerNumber, peerCount);
            row = logicalIndex / rows;
            column = logicalIndex % columns;
            value = rand.nextDouble();
            localMatrix.add((double) row);
            localMatrix.add((double) column);
            localMatrix.add(value);
          }
      }
      // Sending the number of generated items to masterTask
      peer.send(masterTask,
          new IntegerMessage(peer.getPeerName(), localMatrix.size()));
      peer.sync();
      // Superstep 2
      // masterTask counts offset in global array for each peer and sends it.
      if (peer.getPeerName().equals(masterTask)) {
        int offset = 0;
        IntegerMessage received;
        while ((received = peer.getCurrentMessage()) != null) {
          String recieverName = received.getTag();
          int recieverCount = received.getData();
          peer.send(recieverName, new IntegerMessage(masterTask, offset));
          offset += recieverCount;
          generatedCount = offset;
        }
        result = new double[offset];
      }
      peer.sync();
      // Superstep 3
      // Peers are filling the result array
      IntegerMessage received;
      if ((received = peer.getCurrentMessage()) != null) {
        if (received.getTag().equals(masterTask)) {
          int offset = received.getData();
          for (int i = 0; i < localMatrix.size(); i++)
            result[offset + i] = localMatrix.get(i);
        }
      }
    }

    /**
     * Prints useful info about generated matrix: rows, columns, sparsity, items
     * count.
     */
    @Override
    public void cleanup(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, IntegerMessage> peer)
        throws IOException {
      if (peer.getPeerName().equals(masterTask)) {
        peer.write(new Text("Generated matix is"), null);
        peer.write(new Text("Sparsity    = "), new DoubleWritable(sparsity));
        peer.write(new Text("rows        = "), new DoubleWritable(rows));
        peer.write(new Text("columns     = "), new DoubleWritable(columns));
        peer.write(new Text("items count = "), new DoubleWritable(
            generatedCount / 3));
        peer.write(new Text("\nValues:"), null);
        for (int i = 0; i < result.length; i++) {
          peer.write(null, new DoubleWritable(result[i]));
        }
      }
    }

  }

  /**
   * This method copies output from System.out to tmp file from configuration
   */
  static void printOutput(HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(TMP_OUTPUT);
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() > 0) {
        FSDataInputStream in = fs.open(files[i].getPath());
        IOUtils.copyBytes(in, System.out, conf, false);
        in.close();
        break;
      }
    }

    fs.delete(TMP_OUTPUT, true);
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