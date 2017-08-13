package com.kafka.streaming.storm.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
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
public class KafkaSpoutTopology {


    private final BrokerHosts brokerHosts;
    
    private static final int PARALLELISM = 1;
    
    private static final String FILE_EXT =".txt";

    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTopology.class);


    public KafkaSpoutTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology(String zooKeeper,String topic,String groupId,String zkRoot, PropertiesLoader loader) {
        ZkHosts zkHosts=new ZkHosts(zooKeeper);
        
        SpoutConfig kafkaConfig=new SpoutConfig(zkHosts, topic, zkRoot, groupId);
        kafkaConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
        //kafkaConfig.forceFromStart=true;
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout(KafkaSpout.class.getName(), new KafkaSpout(kafkaConfig), PARALLELISM);
        
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
        		// Post rotate action
                .addRotationAction(new MoveFileAction().toDestination(loader.getString(ConsumerEnum.MOVED_PATH.getValue())));
             
        builder.setBolt(com.kafka.streaming.storm.bolt.HdfsBolt.class.getName(), bolt).globalGrouping(KafkaSpout.class.getName());
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

    	PropertiesLoader loader = new PropertiesLoader();
        KafkaSpoutTopology kafkaSpoutTestTopology = new KafkaSpoutTopology(loader.getString(ConsumerEnum.ZOOKEEPER.getValue()));
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology(loader.getString(ConsumerEnum.ZOOKEEPER.getValue()),loader.getString(ConsumerEnum.KAFKA_TOPIC.getValue()),loader.getString(ConsumerEnum.CONSUMER_GROUP.getValue()),loader.getString(ConsumerEnum.ZK_ROOT.getValue()),loader);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaWordCountStorm", config, stormTopology);
        Thread.sleep(10000);
    }
}
