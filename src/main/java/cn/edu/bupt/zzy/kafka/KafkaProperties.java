package cn.edu.bupt.zzy.kafka;

/**
 * @ClassName: KafkaProperties
 * @description: Kafka常用配置文件
 * @author: zzy
 * @date: 2019-04-29 16:04
 * @version: V1.0
 **/
public class KafkaProperties {

//    public static final String ZK = "10.103.247.241:2181";
    public static final String ZK = "hadoop000:2181";

    public static final String TOPIC = "hello_topic";

//    public static final String BROKER_LIST = "10.103.247.241:9092";
    public static final String BROKER_LIST = "hadoop000:9092";

    public static final String GROUP_ID = "test_group1";

}
