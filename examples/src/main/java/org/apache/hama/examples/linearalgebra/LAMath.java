package org.apache.hama.examples.linearalgebra;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.messages.VectorCellMessage;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.linearalgebra.formats.ContractibleMatrix;
import org.apache.hama.examples.linearalgebra.formats.DenseVector;
import org.apache.hama.examples.linearalgebra.formats.Matrix;
import org.apache.hama.examples.linearalgebra.formats.SpMVMatrix;
import org.apache.hama.examples.linearalgebra.formats.SparseVector;
import org.apache.hama.examples.linearalgebra.formats.Vector;
import org.apache.hama.examples.linearalgebra.mappers.Mapper;
import org.apache.hama.examples.linearalgebra.structures.MatrixCell;
import org.apache.hama.examples.linearalgebra.structures.VectorCell;
import org.jfree.util.Log;

/**
 * This class was designed to be responsible for all methods for linear algebra
 * operations. Currently implements only SpMV.
 */
public class LAMath {

  private static Path TMP_OUTPUT = new Path("/tmp/matrix-gen-"
      + System.currentTimeMillis());

  private static HamaConfiguration conf;

  public static String requestedBspTasksString = "bsptask.count";

  private double sparse = 0.5;

  public LAMath() {
    conf = new HamaConfiguration();
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

  /**
   * Multiplies matrix to vector in case of sparse and dense matrix.
   */
  public Vector multiply(Matrix m, Vector v) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (m.getSparsity() < sparse)
      return SparseMatrixVectorMultiplication(m, v);
    else
      return DenseMatrixVectorMultiplication(m, v);
  }

  /**
   * Multiplies sparse matrix to vector.
   */
  public Vector SparseMatrixVectorMultiplication(Matrix m, Vector v)
      throws IOException, InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, SpMV.class);
    // Set the job name
    bsp.setJobName("Sparse matrix vector multiplication");
    bsp.setBspClass(SpMV.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);
    SpMV spmvSolver = new SpMV();
    spmvSolver.setInput(m, v);
    if (LAMath.getRequestedBspTasksCount() != -1) {
      bsp.setNumBspTask(LAMath.getRequestedBspTasksCount());
    } else {
      // Set to maximum
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
      return spmvSolver.getResult();
    } else {
      return null;
    }
  }

  /**
   * Multiplies sparse matrix to vector.
   */
  public Vector DenseMatrixVectorMultiplication(Matrix m, Vector v) {
    throw new UnsupportedOperationException(
        "Dense matrix vector multiplication is not implemented yet.");
  }

  /**
   * This class performs sparse matrix vector multiplication. u = m * v. m -
   * input matrix, u - partial sum, v - input vector.
   */
  private static class SpMV
      extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> {
    private static Mapper mMapper, uMapper, vMapper;
    private static Matrix mLocalArray[];
    private static Vector vLocalArray[];
    private static Matrix m;
    private static Vector v;
    private static int peerCount;
    private static String masterTask;
    private static DenseVector result;

    public void setInput(Matrix matrix, Vector vector) {
      m = matrix;
      v = vector;
    }

    /**
     * In setup we will define strategy and make distribution of matrix and
     * vector.
     */
    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> peer)
        throws IOException, SyncException, InterruptedException {
      super.setup(peer);
      System.out.println("setup");
      masterTask = peer.getPeerName(peer.getNumPeers() / 2);
      peerCount = peer.getNumPeers();
      if (peer.getPeerName().equals(masterTask)) {
        mLocalArray = new Matrix[peerCount];
        vLocalArray = new SparseVector[peerCount];
        SpMVStrategy strategy = new DefaultSpMVStrategy();
        strategy.analyze(m, v, peerCount);
        m = strategy.getMatrixFormat();
        v = strategy.getVectorFormat();
        mMapper = strategy.getMatrixMapper();
        vMapper = strategy.getVMapper();
        uMapper = strategy.getUMapper();
        for (int i = 0; i < peerCount; i++) {
          mLocalArray[i] = strategy.getNewMatrixFormat();
          vLocalArray[i] = new SparseVector(m.getColumns());
        }
        // TODO May be it is not the best idea to store local matrices
        // If better implementation exists please notify the developers.

        // matrix distribution.
        Iterator<MatrixCell> mIterator = m.getDataIterator();
        while (mIterator.hasNext()) {
          MatrixCell mCell = mIterator.next();
          int owner = mMapper.owner(mCell.getRow(), mCell.getColumn());
          mLocalArray[owner].setMatrixCell(mCell);
        }

        // v vector distribution
        Iterator<VectorCell> vIterator = v.getDataIterator();
        while (vIterator.hasNext()) {
          VectorCell vCell = vIterator.next();
          int owner = vMapper.owner(vCell.getIndex());
          vLocalArray[owner].setVectorCell(vCell);
        }
      }

    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> peer)
        throws IOException, SyncException, InterruptedException {
      try {
        System.out.println("bsp");
        peer.sync();
        HashMap<Integer, Integer> rowBackMapping = null;
        List<Integer> nonZeroMatrixColumns = null;
        List<String> peerNameList = Arrays.asList(peer.getAllPeerNames());
        HashMap<Integer, Integer> columnForwardMapping = null;
        SparseVector uLocal = new SparseVector();
        String peerName = peer.getPeerName();
        int peerIndex = peerNameList.indexOf(peerName);
        Matrix mLocal = null;
        try {
          System.out.println("trying to access local matrix index = "+peerIndex);
          mLocal = mLocalArray[peerIndex];
        } catch (Exception e) {
          e.printStackTrace();
        }
        Vector vLocal = vLocalArray[peerIndex];
        uLocal = new SparseVector(m.getRows());

        // Compressing matrix if possible.
        if (mLocal instanceof ContractibleMatrix) {
          ContractibleMatrix contractibleMatrix = (ContractibleMatrix) mLocal;
          contractibleMatrix.compress();
          rowBackMapping = contractibleMatrix.getBackRowMapping();
          columnForwardMapping = contractibleMatrix.getColumnMapping();
          if (peerIndex >= peerCount)
            System.out.println("Peer index greater than count");
          if (vLocal == null)
            System.out.println("This is impossible. Vector is not initialized");
          SparseVector newVLocal = new SparseVector(vLocal.getDimension());
          Iterator<VectorCell> vIterator = vLocal.getDataIterator();
          while (vIterator.hasNext()) {
            VectorCell vCell = vIterator.next();
            int index = vCell.getIndex();
            double value = vCell.getValue();
            if (columnForwardMapping.containsKey(index))
              index = columnForwardMapping.get(index);
            newVLocal.setVectorCell(new VectorCell(index, value));
          }
          vLocal = newVLocal;
        }

        // ***Fanout SuperStep***
        // In this superstep components of input vector v are communicated.
        if (mLocal instanceof SpMVMatrix) {
          SpMVMatrix spmvMatrix = ((SpMVMatrix) mLocal);
          nonZeroMatrixColumns = spmvMatrix.getNonZeroColumns();
        }
        if (nonZeroMatrixColumns == null)
          System.out.println("null from top level " + peerName);
        try {
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
        } catch (Exception e) {
          e.printStackTrace();
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
          if (columnForwardMapping.containsKey(index))
            index = columnForwardMapping.get(index);
          vLocal.setVectorCell(new VectorCell(index, value));
        }
        peer.sync();

        // ***Local computation superstep.***
        // In this superstep we count local contribution to result u = m * v.
        Iterator<MatrixCell> mIterator = mLocal.getDataIterator();
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

        // decompressing local contribution vector if it was compressed.
        if (rowBackMapping != null) {
          SparseVector newU = new SparseVector(uLocal.getDimension());
          Iterator<VectorCell> uIterator = uLocal.getDataIterator();
          while (uIterator.hasNext()) {
            VectorCell uCell = uIterator.next();
            int index = uCell.getIndex();
            double value = uCell.getValue();
            if (rowBackMapping.containsKey(index))
              index = rowBackMapping.get(index);
            newU.setVectorCell(new VectorCell(index, value));
          }
          uLocal = newU;
        }

        // ***Fanin Superstep***
        // In this superstep components of local contributions vector u are
        // communicated
        // and summed.
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
          result = new DenseVector(m.getRows());
          while (peer.getNumCurrentMessages() > 0) {
            VectorCellMessage receivedUCell = peer.getCurrentMessage();
            int index = receivedUCell.getIndex();
            double value = receivedUCell.getValue();
            result.setVectorCell(new VectorCell(index, value));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        Log.error(e.getLocalizedMessage());
      }
    }

    public DenseVector getResult() {
      return result;
    }
  }

}
