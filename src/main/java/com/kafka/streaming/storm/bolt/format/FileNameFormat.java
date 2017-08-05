package com.kafka.streaming.storm.bolt.format;


import backtype.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Formatter interface for determining HDFS file names.
 *
 */
public interface FileNameFormat extends Serializable {

    void prepare(Map conf, TopologyContext topologyContext);

    /**
     * Returns the filename the HdfsBolt will create.
     * @param rotation the current file rotation number (incremented on every rotation)
     * @param timeStamp current time in milliseconds when the rotation occurs
     * @return
     */
    String getName(long rotation, long timeStamp);

    String getPath();
}
