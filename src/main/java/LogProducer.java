import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class LogProducer {

    public static KafkaProducer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.PRODUCER_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConfig.KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConfig.VALUE_SERIALIZER);
        return new KafkaProducer<Long, String>(props);
    }

    public static void runProducer() throws InterruptedException {
        KafkaProducer<Long, String> kafkaProducer = LogProducer.createProducer();
        while (true) {
            long now = System.currentTimeMillis();
            String message = "GET /twiki/bin/rdiff/Main/WebIndex?rev1=1.2&rev2=1.1 HTTP/1.1 200 46373";
            final ProducerRecord<Long, String> producerRecord = new ProducerRecord<Long, String>(
                    KafkaConfig.TOPIC,
                    now,
                    message
            );
            kafkaProducer.send(producerRecord);
            kafkaProducer.flush();
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        LogProducer.runProducer();
    }
}
