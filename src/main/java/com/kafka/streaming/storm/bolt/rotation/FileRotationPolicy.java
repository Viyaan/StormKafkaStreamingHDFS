package com.kafka.streaming.storm.bolt.rotation;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * Used by the HdfsBolt to decide when to rotate files.
 *
 * The HdfsBolt will call the <code>mark()</code> method for every
 * tuple received. If the <code>mark()</code> method returns
 * <code>true</code> the HdfsBolt will perform a file rotation.
 *
 * After file rotation, the HdfsBolt will call the <code>reset()</code>
 * method.
 */
public interface FileRotationPolicy extends Serializable {
    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple The tuple executed.
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    boolean mark(Tuple tuple, long offset);


    /**
     * Called after the HdfsBolt rotates a file.
     *
     */
    void reset();
}
