package cn.edu.bupt.zzy.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName: KafkaProducer
 * @description: Kafka生产者
 * @author: zzy
 * @date: 2019-04-29 16:07
 * @version: V1.0
 **/
public class KafkaProducer extends Thread {

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();

        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        // 序列化类默认为kafka.serializer.DefaultEncoder，
        // 在AsyncProducerConfig中定义
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        // 在SyncProducerConfig中定义，可选0,1，-1三个值，-1最严格，1次之
        properties.put("request.required.acks","1");

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {

        int messageNo = 1;

        while (true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent: " + message);

            messageNo++;

            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
