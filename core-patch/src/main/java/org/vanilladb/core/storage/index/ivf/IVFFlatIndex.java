package org.vanilladb.core.storage.index.ivf;

import java.util.concurrent.CountDownLatch;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

public class IVFFlatIndex extends Index {

    public final static int NUM_CENTROIDS;
    public final static int NUM_PROBE_BUCKETS;
    // public final static int PROBE_THREADS = Runtime.getRuntime().availableProcessors();
    public final static int PROBE_THREADS = 1;

    static {
        NUM_CENTROIDS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_CENTROIDS", 512);
        NUM_PROBE_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_PROBE_BUCKETS", 8);
    }

    public static int numCentroidBlocks(SearchKeyType keyType) {
        // TODO: I thought Buffer.BUFFER_SIZE equals Page.BLOCK_SIZE.
        // They are not. So each buffer can store at most 7 vectors instead of 8.
        // I am planning to flatten
        int vpb = Buffer.BUFFER_SIZE / keyType.get(0).maxSize();
        return (IVFFlatIndex.NUM_CENTROIDS + vpb - 1) / vpb;
    }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        // Assume that the vector field accounts for most of the space
        int vpb = Buffer.BUFFER_SIZE / keyType.get(0).maxSize();
        return numCentroidBlocks(keyType) + (totRecs / vpb) / NUM_CENTROIDS;
    }

    private class ProbeTask extends Task {
        private EuclideanFn distFn;
        private int low, high;  // The assigned task
        private CountDownLatch doneSignal;
        // Accessible from the outside
        double[] dists;
        int[] probedClusters;
        int probeId, probeCount;

        ProbeTask(int n, EuclideanFn distFn, int probeCount, CountDownLatch doneSignal) {
            this.distFn = distFn;
            this.probeCount = probeCount;
            this.doneSignal = doneSignal;
            low = n * IVFFlatIndex.NUM_CENTROIDS / PROBE_THREADS;
            high = (n + 1) * IVFFlatIndex.NUM_CENTROIDS / PROBE_THREADS;
            dists = new double[probeCount];
            probedClusters = new int[probeCount];
            probeId = 0;
        }

        @Override
        public void run() {
            for (int i = low; i < high; ++i) {
                VectorConstant centroid = getCentroidVector(i);
                double dist = distFn.distance(centroid);
                int j = probeId;
                if (probeId < probeCount) ++probeId;
                while (j > 0 && dist < dists[j - 1]) {
                    if (j < probeId) {
                        dists[j] = dists[j - 1];
                        probedClusters[j] = probedClusters[j - 1];
                    }
                    --j;
                }
                if (j < probeId) {
                    dists[j] = dist;
                    probedClusters[j] = i;
                }
            }
            probeCount = probeId; probeId = 0;
            doneSignal.countDown();
        }
    }

    public String centroidName() {
        return ii.indexName() + "_centroids.idx";
    }

    public VectorConstant getCentroidVector(int bucket) {
        int vpb = Buffer.BUFFER_SIZE / keyType.get(0).maxSize();
        int offset = bucket % vpb * keyType.get(0).maxSize();
        Buffer buff = tx.bufferMgr().pin(new BlockId(centroidName(), bucket / vpb));
        VectorConstant v = (VectorConstant)buff.getVal(offset, keyType.get(0));
        tx.bufferMgr().unpin(buff);
        return v;
    }

    public void setCentroidVector(int bucket, VectorConstant centroid) {
        int vpb = Buffer.BUFFER_SIZE / keyType.get(0).maxSize();
        int offset = bucket % vpb * keyType.get(0).maxSize();
        Buffer buff = tx.bufferMgr().pin(new BlockId(centroidName(), bucket / vpb));
        // We assume this method is only called in Load Testbed
        // So the concurrency manager and LSN do not need to be involved
        buff.setVal(offset, centroid, tx.getTransactionNumber(), null);
        tx.bufferMgr().unpin(buff);
    }

    private void probe(VectorConstant query, int probeCount) {
        if (VanillaDb.fileMgr().isFileEmpty(centroidName())) {
            probeId = -1;
            return;
        }
        if (probeCount > IVFFlatIndex.NUM_CENTROIDS)
            throw new RuntimeException();
        EuclideanFn distFn = new EuclideanFn(ii.fieldNames().get(0));
        distFn.setQueryVector(query);

        ProbeTask[] tasks = new ProbeTask[PROBE_THREADS];
        CountDownLatch doneSignal = new CountDownLatch(PROBE_THREADS);
        for (int i = 0; i < PROBE_THREADS; ++i) {
            tasks[i] = new ProbeTask(i, distFn, probeCount, doneSignal);
            VanillaDb.taskMgr().runTask(tasks[i]);
        }
        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double[] dists = new double[probeCount];
        probedClusters = new int[probeCount];
        for (probeId = 0; probeId < probeCount; ++probeId) {
            ProbeTask currentBest = null;
            int i;
            for (i = 0; i < PROBE_THREADS; ++i) {
                if (tasks[i].probeId == tasks[i].probeCount) continue;
                if (currentBest == null
                        || tasks[i].dists[tasks[i].probeId] < currentBest.dists[currentBest.probeId])
                    currentBest = tasks[i];
            }
            double dist = currentBest.dists[currentBest.probeId];

            for (i = probeId; i > 0 && dist < dists[i - 1]; --i) {
                dists[i] = dists[i - 1];
                probedClusters[i] = probedClusters[i - 1];
            }
            dists[i] = dist;
            probedClusters[i] = currentBest.probedClusters[currentBest.probeId];
        }
        probeId = 0;
    }

	private SearchKey searchKey;
    private Schema tableSchema;
    private RecordFile rf;
    private int[] probedClusters;
    private int probeId;

    public IVFFlatIndex(IndexInfo ii, SearchKeyType keyType, Schema schema, Transaction tx) {
        super(ii, keyType, tx);
        tableSchema = schema;
        probeId = -1;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        close();

        searchKey = searchRange.asSearchKey();
        probe((VectorConstant)searchKey.get(0), NUM_PROBE_BUCKETS);
        String tblname = ii.indexName() + probedClusters[0];
        TableInfo ti = new TableInfo(tblname, tableSchema);

        // the underlying record file should not perform logging
        rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();
    }

    @Override
    public boolean next() {
        if (probeId == -1)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '"
                    + ii.indexName() + "'");
        
        while (!rf.next()) {
            rf.close();
            if (++probeId == probedClusters.length) {
                rf = null;
                return false;
            }

            String tblname = ii.indexName() + probedClusters[probeId];
            TableInfo ti = new TableInfo(tblname, tableSchema);

            // the underlying record file should not perform logging
            rf = ti.open(tx, false);
    
            // initialize the file header if needed
            if (rf.fileSize() == 0)
                RecordFile.formatFileHeader(ti.fileName(), tx);
            rf.beforeFirst();
        }
        // System.err.println("next returning true");
        return true;
    }

    @Override
    public RecordId getDataRecordId() {
        return rf.currentRecordId();
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        int i;
        close();

        // "key" has all the fields, not just the key field
        searchKey = key;
        for (i = 0; !(searchKey.get(i) instanceof VectorConstant); ++i) {}
        probe((VectorConstant)searchKey.get(i), 1);
        if (probeId == -1) return;  // Still loading testbed

        String tblname = ii.indexName() + probedClusters[0];
        TableInfo ti = new TableInfo(tblname, tableSchema);

        // the underlying record file should not perform logging
        rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        // insert the data
        i = 0;
        rf.insert();
        for (String fldname : tableSchema.fields()) {
            rf.setVal(fldname, key.get(i++));
        }

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        close();

        searchKey = key;
        probe((VectorConstant)searchKey.get(0), 1);
        if (probeId == -1) return;  // Probably won't happen

        String tblname = ii.indexName() + probedClusters[0];
        if (VanillaDb.fileMgr().isFileEmpty(tblname)) return;
        TableInfo ti = new TableInfo(tblname, tableSchema);

        // the underlying record file should not perform logging
        rf = ti.open(tx, false);
        rf.beforeFirst();

        // log the lofical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();
        
        // delete the specified entry
        while (next())
            if (getDataRecordId().equals(dataRecordId)) {
                rf.delete();
                break;
            }
        
        if (probeId == probedClusters.length)
            System.err.println("Warning: IVF_FLAT delete failed to delete a record");

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void close() {
        if (rf != null) {
            rf.close();
            rf = null;
        }
        probedClusters = null;
        probeId = -1;
    }

    @Override
    public void preLoadToMemory() {
        // TODO: this needs NUM_CENTROIDS / 7 buffers, is it enough?
        int size = numCentroidBlocks(keyType);
        BlockId blk;
        for (int j = 0; j < size; j++) {
            blk = new BlockId(centroidName(), j);
            tx.bufferMgr().pin(blk);
        }
    }

    public Constant getVal(String fldName) {
        return rf.getVal(fldName);
    }

}
