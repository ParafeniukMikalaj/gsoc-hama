package org.apache.hama.examples.linearalgebra;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
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
import org.apache.hama.examples.linearalgebra.util.RandomMatrixGenerator;

/**
 * This class was designed to be responsible for all methods for linear algebra
 * operations. Currently implements only SpMV.
 */
public class LAMath {

  private double sparse = 0.3;

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

    BSPJob bsp = new BSPJob(conf, RandomMatrixGenerator.class);
    // Set the job name
    bsp.setJobName("Sparse matrix vector multiplication");
    bsp.setBspClass(SpMV.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);
    SpMV spmvSolver = new SpMV();
    spmvSolver.setInput(m, v);
    bsp.setNumBspTask(cluster.getMaxTasks());

    if (bsp.waitForCompletion(true)) {
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
  private class SpMV
      extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> {
    private Mapper mMapper, uMapper, vMapper;
    private Matrix mLocalArray[];
    private Vector vLocalArray[];
    private Matrix m;
    private Vector v;
    private int peerCount;
    private String masterTask;
    private DenseVector result;

    public void setInput(Matrix m, Vector v) {
      this.m = m;
      this.v = v;
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
      masterTask = peer.getPeerName(peer.getNumPeers() / 2);
      peerCount = peer.getNumPeers();
      mLocalArray = new Matrix[peerCount];
      vLocalArray = new SparseVector[peerCount];
      SpMVStrategy strategy = new DefaultSpMVStrategy();
      strategy.analyze(m, v, peerCount);
      m = strategy.getMatrixFormat();
      v = strategy.getVectorFormat();
      mMapper = strategy.getMatrixMapper();
      vMapper = strategy.getVMapper();
      uMapper = strategy.getUMapper();
      for (int i = 0; i < peerCount; i++)
        mLocalArray[i] = strategy.getNewMatrixFormat();

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

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> peer)
        throws IOException, SyncException, InterruptedException {
      HashMap<Integer, Integer> rowBackMapping = null;
      List<Integer> nonZeroMatrixColumns = null;
      List<String> peerNameList = Arrays.asList(peer.getAllPeerNames());
      SparseVector uLocal = new SparseVector();
      String peerName = peer.getPeerName();
      int peerIndex = peerNameList.indexOf(peerName);
      Matrix mLocal = mLocalArray[peerIndex];
      Vector vLocal = vLocalArray[peerIndex];

      // Compressing matrix if possible.
      if (mLocal instanceof ContractibleMatrix) {
        ContractibleMatrix contractibleMatrix = (ContractibleMatrix) mLocal;
        contractibleMatrix.compress();
        rowBackMapping = contractibleMatrix.getBackRowMapping();
        HashMap<Integer, Integer> columnForwardMapping = contractibleMatrix
            .getColumnMapping();
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
      if (mLocal instanceof SpMVMatrix)
        // TODO do something if matrix is in not valid format. How to
        // handle exceptions in bsp procedure?
        nonZeroMatrixColumns = ((SpMVMatrix) mLocal).getNonZeroColumns();
      for (Integer columnIndex : nonZeroMatrixColumns) {
        int owner = vMapper.owner(columnIndex);
        if (owner != peerIndex) {
          String destinationPeerName = peerNameList.get(owner);
          VectorCellMessage vMessage = new VectorCellMessage(peerName,
              columnIndex, 0);
          peer.send(destinationPeerName, vMessage);
        }
      }
      peer.sync();

      for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
        VectorCellMessage requestedVMessage = peer.getCurrentMessage();
        int index = requestedVMessage.getIndex();
        double value = vLocal.getCell(index);
        requestedVMessage.setValue(value);
        String senderName = requestedVMessage.getTag().toString();
        peer.send(senderName, requestedVMessage);
      }
      peer.sync();

      for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
        VectorCellMessage receivedVMessage = peer.getCurrentMessage();
        VectorCell vectorCell = new VectorCell(receivedVMessage.getIndex(),
            receivedVMessage.getValue());
        vLocal.setVectorCell(vectorCell);
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
          peer.send(destinationPeerName, message);
        }
      }
      peer.sync();

      for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
        VectorCellMessage receivedUCell = peer.getCurrentMessage();
        int index = receivedUCell.getIndex();
        double value = receivedUCell.getValue();
        double localValue = uLocal.getCell(index);
        double newValue = uLocal.getCell(index) + localValue * value;
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
        for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
          VectorCellMessage receivedUCell = peer.getCurrentMessage();
          int index = receivedUCell.getIndex();
          double value = receivedUCell.getValue();
          result.setVectorCell(new VectorCell(index, value));
        }
      }
    }

    public DenseVector getResult() {
      return result;
    }
  }

}
