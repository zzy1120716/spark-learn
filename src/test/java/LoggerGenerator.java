import org.apache.log4j.Logger;

/**
 * @ClassName: LoggerGenerator
 * @description: 模拟日志产生
 * @author: zzy
 * @date: 2019-05-06 10:05
 * @version: V1.0
 **/
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception {

        int index = 0;
        while (true) {
            Thread.sleep(1000);
//            logger.info("current value is: " + index++);
            logger.info("value: " + index++);
        }

    }

}
