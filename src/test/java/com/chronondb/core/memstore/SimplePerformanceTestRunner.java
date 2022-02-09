package com.chronondb.core.memstore;

import com.chronondb.core.exception.DatabaseGenericException;
import org.apache.commons.lang3.RandomUtils;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class SimplePerformanceTestRunner implements Callable<ExecutionStatus> {

    int chunkSize = 100 * 1000;
    int delayMinMs = 10;
    int delayMaxMs = 200;
    int workTimeSec = 10;
    final int idPrefix;
    Log<Long, Integer> log;
    String namePrefix;


    public SimplePerformanceTestRunner(int idPrefix, Log<Long, Integer> log) {
        this.idPrefix = idPrefix;
        this.log = log;
    }

    public SimplePerformanceTestRunner setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public SimplePerformanceTestRunner setDelayMax(int delayMaxMs) {
        this.delayMaxMs = delayMaxMs;
        return this;
    }

    public SimplePerformanceTestRunner setWorkTimeSec(int workTimeSec) {
        this.workTimeSec = workTimeSec;
        return this;
    }

    @Override
    public ExecutionStatus call() throws Exception {

        var stat = new ExecutionStatus();

        this.namePrefix = Thread.currentThread().getName() + " - ";

        long stopTime = System.currentTimeMillis() + workTimeSec * 1000L;

        try {
            while (!Thread.interrupted() && System.currentTimeMillis() < stopTime) {

                writePackage(Long.MAX_VALUE, stat);

                sleep();
                verifyPackage(stat);
                sleep();
                broadReadAndVerify(stat);
                sleep();
                removePackage(stat);
                sleep();
                vacuum(log, stat);
            }
        } catch (Exception e) {
            stat.crashed = true;
            stat.crashMessage = e.getMessage();
            stat.crashException = e;
        }

        return stat;
    }

    private void broadReadAndVerify(ExecutionStatus stat) throws Exception {
        long start = System.currentTimeMillis();

        var result = log.get(Long.MIN_VALUE, Long.MAX_VALUE);

        long opTime = System.currentTimeMillis() - start;

        Set<Long> qrs = result.stream().map(LogItem::getId).collect(Collectors.toSet());

        for (long i = (long) idPrefix * chunkSize; i < (long) (idPrefix + 1) * chunkSize; i++) {
            if (!qrs.contains(i)) throw new Exception("Broad read verification failed");
        }

        stat.readOps++;
        stat.readOpsTime += opTime;
        stat.readOpsRecs += result.size();

    }

    private void vacuum(Log<Long, Integer> log, ExecutionStatus stats) {
        long start = System.currentTimeMillis();

        stats.vacuumBlocks += log.vacuum();

        stats.vacuumOps++;
        stats.vacuumOpsTime += System.currentTimeMillis() - start;

    }

    private void verifyPackage(ExecutionStatus stats) throws Exception {
        long start = System.currentTimeMillis();

        for (long i = (long) idPrefix * chunkSize; i < (long) (idPrefix + 1) * chunkSize; i++) {
            if (log.get(i) == null) throw new Exception("Verification failed for key " + i);
        }

        stats.verifyOps += chunkSize;
        stats.verifyOpsTime += System.currentTimeMillis() - start;
    }

    private void sleep() {
        try {
            Thread.sleep(RandomUtils.nextInt(delayMinMs, delayMaxMs));
        } catch (InterruptedException ignored) {
        }
    }

    private void removePackage(ExecutionStatus stats) throws DatabaseGenericException {
        long start = System.currentTimeMillis();

        for (long i = (long) idPrefix * chunkSize; i < (long) (idPrefix + 1) * chunkSize; i++) {
            log.remove(i);
        }

        stats.removeOps += chunkSize;
        stats.removeOpsTime += System.currentTimeMillis() - start;
    }

    private void writePackage(long ttl, ExecutionStatus stats) throws DatabaseGenericException {
        long start = System.currentTimeMillis();

        for (long i = (long) idPrefix * chunkSize; i < (long) (idPrefix + 1) * chunkSize; i++) {
            log.add(i, ttl, null);
        }

        stats.writeOps += chunkSize;
        stats.writeOpsTime += System.currentTimeMillis() - start;
    }


}
