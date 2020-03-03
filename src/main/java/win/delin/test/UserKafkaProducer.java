package win.delin.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UserKafkaProducer extends Thread {
    private KafkaProducer<Integer, String> producer;
    private String topic;
//    private List<String> datas;
    private String minus;
    public UserKafkaProducer(String brokerListStr, String topic, String minus) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerListStr);
        props.put("bootstrap.servers", brokerListStr);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("compression.type", "snappy");
        producer = new KafkaProducer(props);
        this.topic = topic;
        this.minus = minus;
    }

    @Override
    public void run() {
        if (!"x".equals(minus)){
            List<JSONObject> dataArr = new ArrayList<>();
            for (String data : MockDataFromFileToKafka.datas) {
                JSONObject dataJson = JSONObject.parseObject(data);
                dataArr.add(dataJson);
            }
            while (true) {
                for (JSONObject data : dataArr) {
                    data.put("collectorReceiptTime", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now().minus(Long.valueOf(minus), ChronoUnit.DAYS)));
                    producer.send(new ProducerRecord(topic, data.toJSONString()));
                    MockDataFromFileToKafka.TOTAL.add(1L);
                }
            }
        } else {
            while (true) {
                for (String data : MockDataFromFileToKafka.datas) {
                    producer.send(new ProducerRecord(topic, data));
                    MockDataFromFileToKafka.TOTAL.add(1L);
                }
            }
        }
    }
}
