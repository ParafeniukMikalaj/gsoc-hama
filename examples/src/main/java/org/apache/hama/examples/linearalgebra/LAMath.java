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

public class LAMath {

  private double sparse = 0.3;

  public Vector multiply(Matrix m, Vector v) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (m.getSparsity() < sparse)
      return SparseMatrixVectorMultiplication(m, v);
    else
      return DenseMatrixVectorMultiplication(m, v);
  }

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

  public Vector DenseMatrixVectorMultiplication(Matrix m, Vector v) {
    return null;
  }

  private class SpMV
      extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> {
    private Mapper matrixMapper, uMapper, vMapper;
    private Matrix localMatrices[];
    private Vector localV[];
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
      SpMVStrategy strategy = new DefaultSpMVStrategy();
      strategy.analyze(m, v, peerCount);
      m = strategy.getMatrixFormat();
      v = strategy.getVectorFormat();
      matrixMapper = strategy.getMatrixMapper();
      vMapper = strategy.getVMapper();
      uMapper = strategy.getUMapper();
      localMatrices = new Matrix[peerCount];
      localV = new SparseVector[peerCount];
      for (int i = 0; i < peerCount; i++)
        localMatrices[i] = strategy.getNewMatrixFormat();

      // matrix distribution.
      Iterator<MatrixCell> matrixIterator = m.getDataIterator();
      while (matrixIterator.hasNext()) {
        MatrixCell cell = matrixIterator.next();
        int peerNumber = matrixMapper.owner(cell.getRow(), cell.getColumn());
        localMatrices[peerNumber].setMatrixCell(cell);
      }

      // v vector distribution
      Iterator<VectorCell> vIterator = v.getDataIterator();
      while (vIterator.hasNext()) {
        VectorCell cell = vIterator.next();
        int peerNumber = vMapper.owner(cell.getPosition());
        localV[peerNumber].setVectorCell(cell);
      }

      for (int i = 0; i < peerCount; i++)
        if (localMatrices[i] instanceof ContractibleMatrix)
          ((ContractibleMatrix) localMatrices[i]).compress();

    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, VectorCellMessage> peer)
        throws IOException, SyncException, InterruptedException {
      HashMap<Integer, Integer> backRowMapping = null;
      List<Integer> nonZeroColumns = null;
      List<String> peerNamesList = Arrays.asList(peer.getAllPeerNames());
      SparseVector uLocal = new SparseVector();
      String peerName = peer.getPeerName();
      int peerIndex = peerNamesList.indexOf(peerName);
      Matrix localMatrix = localMatrices[peerIndex];
      Vector vLocal = localV[peerIndex];

      // Compressing matrix if possible.
      if (localMatrix instanceof ContractibleMatrix) {
        ContractibleMatrix tmpMatrix = (ContractibleMatrix) localMatrix;
        tmpMatrix.compress();
        backRowMapping = tmpMatrix.getBackRowMapping();
        HashMap<Integer, Integer> forwardRowMapping = tmpMatrix
            .getColumnMapping();
        SparseVector newV = new SparseVector(vLocal.getDimension());
        Iterator<VectorCell> vIterator = vLocal.getDataIterator();
        while (vIterator.hasNext()) {
          VectorCell cell = vIterator.next();
          int index = cell.getPosition();
          double value = cell.getValue();
          if (forwardRowMapping.containsKey(index))
            index = forwardRowMapping.get(index);
          newV.setVectorCell(new VectorCell(index, value));
        }
        vLocal = newV;
      }

      // ***Fanout SuperStep***
      if (localMatrix instanceof SpMVMatrix)
        nonZeroColumns = ((SpMVMatrix) localMatrix).getNonZeroColumns();
      for (Integer index : nonZeroColumns) {
        int owner = vMapper.owner(index);
        if (owner != peerIndex) {
          String destinationPeerName = peerNamesList.get(owner);
          VectorCellMessage message = new VectorCellMessage(peerName, index, 0);
          peer.send(destinationPeerName, message);
        }
      }
      peer.sync();

      for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
        VectorCellMessage requestedCell = peer.getCurrentMessage();
        int position = requestedCell.getIndex();
        double value = vLocal.getCell(position);
        requestedCell.setValue(value);
        String sender = requestedCell.getTag().toString();
        peer.send(sender, requestedCell);
      }
      peer.sync();

      for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
        VectorCellMessage receivedCell = peer.getCurrentMessage();
        VectorCell cell = new VectorCell(receivedCell.getIndex(),
            receivedCell.getValue());
        vLocal.setVectorCell(cell);
      }
      peer.sync();

      // ***Local computation superstep.***
      Iterator<MatrixCell> matrixIterator = localMatrix.getDataIterator();
      while (matrixIterator.hasNext()) {
        MatrixCell mCell = matrixIterator.next();
        int row = mCell.getRow();
        int column = mCell.getColumn();
        double mValue = mCell.getValue();
        double vValue = vLocal.getCell(column);
        double newValue = uLocal.getCell(row) + mValue * vValue;
        VectorCell rCell = new VectorCell(row, newValue);
        uLocal.setVectorCell(rCell);
      }

      // decompressing local contribution vector if it was compressed.
      if (backRowMapping != null) {
        SparseVector newU = new SparseVector(uLocal.getDimension());
        Iterator<VectorCell> uIterator = uLocal.getDataIterator();
        while (uIterator.hasNext()) {
          VectorCell cell = uIterator.next();
          int index = cell.getPosition();
          double value = cell.getValue();
          if (backRowMapping.containsKey(index))
            index = backRowMapping.get(index);
          newU.setVectorCell(new VectorCell(index, value));
          uLocal = newU;
        }
      }

      // ***Fanin Superstep***
      Iterator<VectorCell> uIterator = uLocal.getDataIterator();
      while (uIterator.hasNext()) {
        VectorCell cell = uIterator.next();
        int index = cell.getPosition();
        double value = cell.getValue();
        int owner = uMapper.owner(index);
        if (owner != peerIndex) {
          String destinationPeerName = peerNamesList.get(owner);
          VectorCellMessage message = new VectorCellMessage(peerName, index,
              value);
          peer.send(destinationPeerName, message);
        }
      }
      peer.sync();

      for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
        VectorCellMessage receivedCell = peer.getCurrentMessage();
        int index = receivedCell.getIndex();
        double value = receivedCell.getValue();
        double localValue = uLocal.getCell(index);
        double newValue = uLocal.getCell(index) + localValue * value;
        VectorCell rCell = new VectorCell(index, newValue);
        uLocal.setVectorCell(rCell);
      }
      Iterator<VectorCell> localContributionIterator = uLocal.getDataIterator();
      while (localContributionIterator.hasNext()) {
        VectorCell cell = localContributionIterator.next();
        peer.send(
            masterTask,
            new VectorCellMessage(peerName, cell.getPosition(), cell.getValue()));
      }
      peer.sync();

      if (peer.getPeerName().equals(masterTask)) {
        result = new DenseVector(m.getRows());
        for (int i = 0; i < peer.getNumCurrentMessages(); i++) {
          VectorCellMessage receivedCell = peer.getCurrentMessage();
          int index = receivedCell.getIndex();
          double value = receivedCell.getValue();
          result.setVectorCell(new VectorCell(index, value));
        }
      }
    }

    public DenseVector getResult() {
      return result;
    }
  }

}
