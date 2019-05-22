package cn.edu.bupt.zzy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @ClassName: StreamingWordCountApp
 * @description: 使用Java开发Spark Streaming应用程序
 * @author: zzy
 * @date: 2019-05-07 14:29
 * @version: V1.0
 **/
public class StreamingWordCountApp {
    
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("StreamingWordCountApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 创建一个DStream(hostname + port)
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop000", 9999);

        JavaPairDStream<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split("\t")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((x, y) -> x + y);

        // 输出到控制台
        counts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
