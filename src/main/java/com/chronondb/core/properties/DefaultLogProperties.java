package com.chronondb.core.properties;

/**
 * Default and simple implementation of a LogProperties
 */
public class DefaultLogProperties implements LogProperties {

    /**
     * Log block size, ms
     */
    public static final int BLOCK_SIZE = 1000;

    /**
     * Delay for block to be vacuumed, ms
     */
    public static final int BLOCK_VACUUM_DELAY_MS = 1000;

    /**
     * Delay for block to be vacuumed, in block sizes
     */
    public static final int BLOCK_VACUUM_DELAY_BLOCKSIZE = 2;

    /**
     * How long to wait for lock before reject PUT operation. GC may be the cause
     */
    public static final int LOCK_THRESHOLD_MS = 100;

    int blockSize = BLOCK_SIZE;
    int blockVacuumDelayMs = BLOCK_VACUUM_DELAY_MS;
    int blockVacuumDelayBlocksize = BLOCK_VACUUM_DELAY_BLOCKSIZE;
    int lockThresholdMs = LOCK_THRESHOLD_MS;

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public int getBlockVacuumDelayInMs() {
        return blockVacuumDelayMs;
    }

    @Override
    public int getBlockVacuumDelayInBlockSize() {
        return blockVacuumDelayBlocksize;
    }

    @Override
    public int getLockThreshold() {
        return lockThresholdMs;
    }

    /**
     * Set's block size.
     *
     * WARNING: Changing this parameter on a fly will corrupt Log until dynamic block size is implemented
     *
     * @param blockSize Log block size in ms
     * @return Instance
     */
    public DefaultLogProperties setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * Define how long to wait before vacuuming the block
     *
     * @param blockVacuumDelayMs how long to wait before vacuuming the block in ms
     * @return Instance
     */
    public DefaultLogProperties setBlockVacuumDelayMs(int blockVacuumDelayMs) {
        this.blockVacuumDelayMs = blockVacuumDelayMs;
        return this;
    }

    /**
     * Define how long to wait before vacuuming the block
     *
     * @param blockVacuumDelayBlocksize Delay for block to be vacuumed, in block sizes
     * @return Instance
     */
    public DefaultLogProperties setBlockVacuumDelayBlocksize(int blockVacuumDelayBlocksize) {
        this.blockVacuumDelayBlocksize = blockVacuumDelayBlocksize;
        return this;
    }

    /**
     * Define, how long to wait for lock before reject PUT operation. GC may be the cause
     *
     * @param lockThresholdMs How long to wait for lock before reject PUT operation in ms.
     * @return Instance
     */
    public DefaultLogProperties setLockThresholdMs(int lockThresholdMs) {
        this.lockThresholdMs = lockThresholdMs;
        return this;
    }
}
