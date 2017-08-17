package com.kafka.streaming.storm.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringKeyValueScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.streaming.storm.bolt.format.DefaultFileNameFormat;
import com.kafka.streaming.storm.bolt.format.DelimitedRecordFormat;
import com.kafka.streaming.storm.bolt.format.FileNameFormat;
import com.kafka.streaming.storm.bolt.format.RecordFormat;
import com.kafka.streaming.storm.bolt.rotation.FileRotationPolicy;
import com.kafka.streaming.storm.bolt.rotation.FileSizeRotationPolicy;
import com.kafka.streaming.storm.bolt.rotation.FileSizeRotationPolicy.Units;
import com.kafka.streaming.storm.bolt.rotation.TimedRotationPolicy;
import com.kafka.streaming.storm.bolt.sync.CountSyncPolicy;
import com.kafka.streaming.storm.bolt.sync.SyncPolicy;
import com.kafka.streaming.storm.common.rotation.MoveFileAction;
import com.kafka.streaming.storm.utils.ConsumerEnum;
import com.kafka.streaming.storm.utils.PropertiesLoader;



/**
 * @author Viyaan
 */
public class KafkaTopology {

    private static final int PARALLELISM = 1;
    
    private static final String FILE_EXT =".txt";

    public static final Logger LOG = LoggerFactory.getLogger(KafkaTopology.class);

    public static StormTopology buildTopology() throws Exception {
    	PropertiesLoader loader = new PropertiesLoader();
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout(KafkaSpout.class.getName(), new KafkaSpout(configureKafkaSpout(loader)), PARALLELISM);
        BaseRichBolt hdfsBolt =configureHdfsBolt(loader);
        builder.setBolt(com.kafka.streaming.storm.bolt.HdfsBolt.class.getName(), hdfsBolt).globalGrouping(KafkaSpout.class.getName());
        return builder.createTopology();
    }
    
    public static Config configureStorm() {
    	Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        return config;
    }
    
    public static SpoutConfig configureKafkaSpout(PropertiesLoader loader) {
    	ZkHosts zkHosts=new ZkHosts(loader.getString(ConsumerEnum.ZOOKEEPER.getValue()));
        SpoutConfig kafkaConfig=new SpoutConfig(zkHosts, loader.getString(ConsumerEnum.KAFKA_TOPIC.getValue()), loader.getString(ConsumerEnum.ZK_ROOT.getValue()), loader.getString(ConsumerEnum.CONSUMER_GROUP.getValue()));
        kafkaConfig.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
        return kafkaConfig;
    }
    
    public static BaseRichBolt configureHdfsBolt(PropertiesLoader loader) {
        SyncPolicy syncPolicy = new CountSyncPolicy(Integer.parseInt(loader.getString(ConsumerEnum.BOLT_BATCH_SIZE.getValue())));
        FileRotationPolicy sizeRotationPolicy =     new FileSizeRotationPolicy(Float.valueOf(loader.getString(ConsumerEnum.SIZE_ROTATION.getValue())), Units.MB);
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(Float.valueOf(loader.getString(ConsumerEnum.TIME_ROTATION.getValue())), TimedRotationPolicy.TimeUnit.MINUTES);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(loader.getString(ConsumerEnum.HDFS_FILE_PATH.getValue())).withExtension(FILE_EXT);
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(loader.getString(ConsumerEnum.FIELD_DELIMITER.getValue()));
        
        com.kafka.streaming.storm.bolt.HdfsBolt bolt = new com.kafka.streaming.storm.bolt.HdfsBolt()
                .withFsUrl(loader.getString(ConsumerEnum.HDFS_FILE_URL.getValue()))
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .addRotationAction(new MoveFileAction().toDestination(loader.getString(ConsumerEnum.MOVED_PATH.getValue())));
        
        return bolt;
    }

    public static void main(String[] args) throws Exception {

        StormTopology stormTopology = buildTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Kafka-Storm-HDFS-Topology", configureStorm(), stormTopology);
    }
}
