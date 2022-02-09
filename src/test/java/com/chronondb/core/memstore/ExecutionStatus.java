package com.chronondb.core.memstore;

public class ExecutionStatus {
    boolean crashed;

    String crashMessage;
    Exception crashException;

    int writeOps = 0;
    int writeOpsTime = 0;

    int verifyOps = 0;
    int verifyOpsTime = 0;

    int readOps = 0;
    int readOpsTime = 0;
    public int readOpsRecs;

    int removeOps = 0;
    int removeOpsTime = 0;

    int vacuumOps = 0;
    int vacuumBlocks = 0;
    int vacuumOpsTime = 0;

    public void append(ExecutionStatus stat) {
        this.writeOps += stat.writeOps;
        this.writeOpsTime += stat.writeOpsTime;

        this.verifyOps += stat.verifyOps;
        this.verifyOpsTime += stat.verifyOpsTime;

        this.readOps += stat.readOps;
        this.readOpsTime += stat.readOpsTime;
        this.readOpsRecs += stat.readOpsRecs;

        this.removeOps += stat.removeOps;
        this.removeOpsTime += stat.removeOpsTime;

        this.vacuumOps += stat.vacuumOps;
        this.vacuumBlocks += stat.vacuumBlocks;
        this.verifyOpsTime += stat.vacuumOpsTime;
    }

    public void print() {
        System.out.println("Write is " + 1000L * this.writeOps / noZero(this.writeOpsTime) + " TPS");
        System.out.println("Single GET is " + 1000L * this.verifyOps / noZero(this.verifyOpsTime) + " TPS");
        System.out.println("Broad GET is " + this.readOpsTime / noZero(this.readOps) + " ms avg. with " + 1000L * this.readOpsRecs / this.readOpsTime + " records per sec.");
        System.out.println("Remove is " + 1000L * this.removeOps / noZero(this.removeOpsTime) + " TPS");
        System.out.println("VACUUM did " + this.vacuumOps + " operations in " + this.vacuumOpsTime + " ms, collected " + this.vacuumBlocks + " blocks");
    }

    private int noZero(int x) {
        return (x == 0) ? 1 : x;
    }
}
