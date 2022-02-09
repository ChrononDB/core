package com.chronondb.core.properties;

/**
 * Log properties provider.
 *
 * WARNING: Mutable settings can be the problem for now
 */
public interface LogProperties {
    /**
     * Block size in ms
     *
     * @return block size in ms
     */
    int getBlockSize();

    /**
     * How long to vait for block vacuuming in ms
     *
     * @return How long to vait for block vacuuming in ms
     */
    int getBlockVacuumDelayInMs();

    /**
     * Protection to the block from vacuuming in blocks
     *
     * @return How long to wait before vacuum in block sizes
     */
    int getBlockVacuumDelayInBlockSize();

    /**
     * How long to wait in PUT operation for lock for block rotation in ms.
     *
     * @return Max PUT op timeout for lock for block rotation
     */
    int getLockThreshold();
}
