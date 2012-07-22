package org.apache.hama.examples.linearalgebra;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.messages.VectorCellMessage;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.linearalgebra.formats.CRSMatrix;
import org.apache.hama.examples.linearalgebra.formats.FileMatrix;
import org.apache.hama.examples.linearalgebra.formats.FileVector;
import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.SparseVector;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.mappers.Mapper;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;
import org.apache.log4j.Logger;

public class SpMV {

  private static HamaConfiguration conf;
  private static final String outputPathString = "spmv.outputpath";
  private static final String inputMatrixPathString = "spmv.inputmatrixpath";
  private static final String inputVectorPathString = "spmv.inputvectorpath";
  private static String requestedBspTasksString = "bsptask.count";
  private static final String spmvSuffix = "/spmv/";
  private static Logger log = Logger.getLogger(SpMV.class);

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
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);

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
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> {
    private static String masterTask;
    private static FileMatrix m;
    private static FileVector v;
    private static FileVector output;
    private static FileMatrix mLocalArray[];
    private static FileVector vLocalArray[];
    private static int peerCount;
    private static Mapper mMapper, uMapper, vMapper;

    /**
     * In setup we will define strategy and make distribution of matrix and
     * vector.
     */
    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> peer)
        throws IOException, SyncException, InterruptedException {
      super.setup(peer);
      masterTask = peer.getPeerName(peer.getNumPeers() / 2);
      if (peer.getPeerName().equals(masterTask)) {
        peerCount = peer.getNumPeers();
        m = new FileMatrix();
        m.setPathString(getInputMatrixPath());
        v = new FileVector();
        v.setPathString(getInputVectorPath());
        output = new FileVector();
        output.setPathString(getOutputPath());
        mLocalArray = new FileMatrix[peerCount];
        vLocalArray = new FileVector[peerCount];
        for (int i = 0; i < peerCount; i++) {
          mLocalArray[i] = new FileMatrix();
          vLocalArray[i] = new FileVector();
        }
        SpMVStrategy strategy = new DefaultSpMVStrategy();
        /*
         * This hardcode needs to be removed in future after I'll create an idea
         * how to store row and column number information in sequence files.
         */
        Matrix tmpM = new CRSMatrix(100, 100);
        Vector tmpV = new SparseVector(100);
        strategy.analyze(tmpM, tmpV, peerCount);
        mMapper = strategy.getMatrixMapper();
        uMapper = strategy.getUMapper();
        vMapper = strategy.getVMapper();
      }
      peer.sync();
    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> peer)
        throws IOException, SyncException, InterruptedException {
      try {

        // ***Distribution of matrix and vector based on Sequence File***
        if (peer.getPeerName().equals(masterTask)) {
          // matrix distribution
          Iterator<MatrixCell> mIterator = m.getDataIterator();
          while (mIterator.hasNext()) {
            MatrixCell mCell = mIterator.next();
            int owner = mMapper.owner(mCell.getRow(), mCell.getColumn());
            mLocalArray[owner].writeCell(mCell);
          }

          // v vector distribution
          Iterator<VectorCell> vIterator = v.getDataIterator();
          while (vIterator.hasNext()) {
            VectorCell vCell = vIterator.next();
            int owner = vMapper.owner(vCell.getIndex());
            vLocalArray[owner].writeCell(vCell);
          }
        }
        peer.sync();

        List<String> peerNameList = Arrays.asList(peer.getAllPeerNames());
        String peerName = peer.getPeerName();
        int peerIndex = peerNameList.indexOf(peerName);
        FileMatrix mLocal = mLocalArray[peerIndex];
        FileVector vLocalFile = vLocalArray[peerIndex];
        SparseVector uLocal = new SparseVector();
        SparseVector vLocal = new SparseVector();
        Iterator<VectorCell> vIterator = vLocalFile.getDataIterator();
        while (vIterator.hasNext())
          vLocal.setVectorCell(vIterator.next());

        // ***Fanout SuperStep***
        // In this superstep components of input vector v are communicated.

        HashSet<Integer> nonZeroMatrixColumns = new HashSet<Integer>();
        Iterator<MatrixCell> mIterator = mLocal.getDataIterator();
        while (mIterator.hasNext()) {
          MatrixCell mCell = mIterator.next();
          nonZeroMatrixColumns.add(mCell.getColumn());
        }
        for (Integer columnIndex : nonZeroMatrixColumns) {
          int owner = vMapper.owner(columnIndex);
          if (owner != peerIndex) {
            String destinationPeerName = peerNameList.get(owner);
            VectorCellMessage vMessage = new VectorCellMessage(peerName,
                columnIndex, 0);
            System.out.println(peerName + " requested " + destinationPeerName
                + " v[" + columnIndex + "]");
            peer.send(destinationPeerName, vMessage);
          }
        }
        peer.sync();

        while (peer.getNumCurrentMessages() > 0) {
          VectorCellMessage requestedVMessage = peer.getCurrentMessage();
          int index = requestedVMessage.getIndex();
          double value = vLocal.getCell(index);
          requestedVMessage.setValue(value);
          String senderName = requestedVMessage.getTag().toString();
          System.out.println(peerName + " sended " + senderName + " v[" + index
              + "]");
          peer.send(senderName, requestedVMessage);
        }
        peer.sync();

        while (peer.getNumCurrentMessages() > 0) {
          VectorCellMessage receivedVMessage = peer.getCurrentMessage();
          VectorCell vectorCell = new VectorCell(receivedVMessage.getIndex(),
              receivedVMessage.getValue());
          System.out.println(peerName + " received " + " v["
              + vectorCell.getIndex() + "]");
          int index = vectorCell.getIndex();
          double value = vectorCell.getValue();
          vLocal.setVectorCell(new VectorCell(index, value));
        }
        peer.sync();

        // ***Local computation superstep.***
        // In this superstep we count local contribution to result u = m * v.
        mIterator = mLocal.getDataIterator();
        while (mIterator.hasNext()) {
          MatrixCell mCell = mIterator.next();
          int row = mCell.getRow();
          int column = mCell.getColumn();
          double mValue = mCell.getValue();
          double vValue = vLocal.getCell(column);
          double newValue = uLocal.getCell(row) + mValue * vValue;
          VectorCell uCell = new VectorCell(row, newValue);
          uLocal.setVectorCell(uCell);
        }

        // ***Fanin Superstep***
        // In this superstep components of local contributions vector u are
        // communicated
        // and summed.

        // Sending non-local inner products
        Iterator<VectorCell> uIterator = uLocal.getDataIterator();
        while (uIterator.hasNext()) {
          VectorCell uCell = uIterator.next();
          int index = uCell.getIndex();
          double value = uCell.getValue();
          int owner = uMapper.owner(index);
          if (owner != peerIndex) {
            String destinationPeerName = peerNameList.get(owner);
            VectorCellMessage message = new VectorCellMessage(peerName, index,
                value);
            System.out.println(peerName + " sending u[" + index + "] to "
                + destinationPeerName);
            peer.send(destinationPeerName, message);
          }
        }
        peer.sync();

        //adding received non-local products
        while (peer.getNumCurrentMessages() > 0) {
          VectorCellMessage receivedUCell = peer.getCurrentMessage();
          int index = receivedUCell.getIndex();
          double value = receivedUCell.getValue();
          double newValue = uLocal.getCell(index) + value;
          VectorCell rCell = new VectorCell(index, newValue);
          uLocal.setVectorCell(rCell);
        }
        uIterator = uLocal.getDataIterator();
        while (uIterator.hasNext()) {
          VectorCell uCell = uIterator.next();
          if (uMapper.owner(uCell.getIndex()) == peerIndex)
            peer.send(
                masterTask,
                new VectorCellMessage(peerName, uCell.getIndex(), uCell
                    .getValue()));
        }
        peer.sync();

        // master task constructs single result vector from received messages.
        if (peer.getPeerName().equals(masterTask)) {
          while (peer.getNumCurrentMessages() > 0) {
            VectorCellMessage receivedUCell = peer.getCurrentMessage();
            int index = receivedUCell.getIndex();
            double value = receivedUCell.getValue();
            output.writeCell(new VectorCell(index, value));
          }
          output.finishWriting();
        }

      } catch (Exception e) {
        log.error(e.getLocalizedMessage());
      }

    }

  }
}
