package com.kafka.streaming.storm.utils;

/**
 * Created by viyaan
 */
public enum ConsumerEnum {





    BROKER_LIST("metadata.broker.list"),
    SERIALIZER("serializer.class"),
    KAFKA_TOPIC("kafka.topic"),
    ZOOKEEPER("zookeeper.host"),
    CONSUMER_GROUP("consumer.groupid"),
	ZK_ROOT("zk.root"),
	FIELD_DELIMITER("bolt.hdfs.field.delimiter"),
	BOLT_BATCH_SIZE("bolt.hdfs.batch.size"),
	HDFS_FILE_URL("bolt.hdfs.file.system.url"),
	HDFS_FILE_PATH("bolt.hdfs.wip.file.path"),
	SIZE_ROTATION("bolt.hdfs.file.rotation.size.in.mb"),
	TIME_ROTATION("bolt.hdfs.file.rotation.time.min"),
	MOVED_PATH("bolt.hdfs.finished.file.path");

    private String value;

    ConsumerEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
