package cn.edu.bupt.zzy.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: KafkaConsumer
 * @description: Kafka消费者
 * @author: zzy
 * @date: 2019-04-29 22:39
 * @version: V1.0
 **/
public class KafkaConsumer extends Thread {

    private String  topic;

    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnector() {

        Properties properties = new Properties();

        properties.put("zookeeper.connect", KafkaProperties.ZK);

        // group.id的值自己定义一个即可
        properties.put("group.id", KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConnector();

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        // topicCountMap.put(topic, 1);
        // topicCountMap.put(topic, 1);     // 可以拿多个topic的数据，本例是一个

        // String: topic
        // List<KafkaStream<byte[], byte[]>> 对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = consumer.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = messageStream.get(topic).get(0);// 获取我们每次接收到的数据

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("Recv: " + message);
        }

    }
}
