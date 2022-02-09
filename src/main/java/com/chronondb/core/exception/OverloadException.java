package com.chronondb.core.exception;

/**
 * DB can't keep up to load or unplanned pause happened (due to GC or planner) which break the thresholds
 */
public class OverloadException extends DatabaseGenericException {
    /**
     * Set exception message
     * @param m message
     */
    public OverloadException(String m) {
        super(m);
    }
}
