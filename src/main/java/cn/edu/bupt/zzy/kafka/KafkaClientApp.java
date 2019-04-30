package cn.edu.bupt.zzy.kafka;

/**
 * @ClassName: KafkaClientApp
 * @description: Kafka Java API测试
 * @author: zzy
 * @date: 2019-04-29 16:57
 * @version: V1.0
 **/
public class KafkaClientApp {

    public static void main(String[] args) {

        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();

    }

}
