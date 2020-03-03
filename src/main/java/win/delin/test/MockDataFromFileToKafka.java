package win.delin.test;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * @author delin
 * @date 2019/11/14
 */
public class MockDataFromFileToKafka {
    Logger LOG = Logger.getLogger("MockDataFromFileToKafka");
    public static LongAdder TOTAL = new LongAdder();
    public static long START = System.currentTimeMillis();
    public static List<String> datas;
    public static void main(String[] args) throws Exception{
        if (args.length < 4) {
            System.out.println("Usage：broker-list,topic,filePath,minusDay[,threadSize]");
            System.out.println("java -jar xxx.jar " +
                    "1.1.1.1:9092 " +
                    "topicName /root/aaa.log 1");
            return;
        }
        String brokerListStr = args[0];
        String topic = args[1];
        String filepath = args[2];
        String days = args[3];
        int threadSize = 1;
        if (args.length > 4) {
            threadSize = Integer.valueOf(args[4]);
        }
        System.out.println("brokerList : " + brokerListStr );
        System.out.println("to topic : " + topic);
        System.out.println("mock filepath : " + filepath);
        System.out.println("minus days : " + days);

        datas = FileUtils.readLines(new File(filepath));
        for (int i = 0; i < threadSize; i++) {
            System.out.println("启动" + i + "号线程");
            UserKafkaProducer producer = new UserKafkaProducer(brokerListStr,topic, days);
            producer.start();
        }

        while (true) {
            Thread.sleep(5000);
            long endTime = System.currentTimeMillis();
            long spent = endTime - START;
            System.out.println("Total: " + TOTAL.longValue() + ", " + "Speed: " + String.format("%.2f", TOTAL.longValue()/(spent * 1.0d /1000)) + " eps");
            datas = FileUtils.readLines(new File(filepath));
            System.out.println("Reloaded file:"+filepath);
        }
    }
}
