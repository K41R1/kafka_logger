import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LogConsumer {
    final static String TOPIC = "test_datalab";
    final static  String BROKERS = "localhost:9092"; // a.k.a BOOTSTRAP_SERVER

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaLogConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords;
            consumerRecords = consumer.poll(1000);
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s)\n", record.key(), record.value());
            });
            consumer.commitAsync();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        runConsumer();
    }
}
