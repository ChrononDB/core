package com.chronondb.core.exception;

/**
 * Generic DB exception
 */
public class DatabaseGenericException extends Exception {
    /**
     * Set exception message
     * @param m message
     */
    public DatabaseGenericException(String m) {
        super(m);
    }
}
