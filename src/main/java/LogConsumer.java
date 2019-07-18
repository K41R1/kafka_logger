import ch.qos.logback.classic.Logger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LogConsumer {

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GROUP_ID_0);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConfig.KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConfig.VALUE_DESERIALIZER);

        final Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);

        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException, IOException {
        final Consumer<Long, String> consumer = createConsumer();
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<Long, String> record: consumerRecords) {
                Logger logger = (Logger) LoggerFactory.getLogger("consumer.hdfs.logger");
                logger.info(String.format("Consumer Record:(%d, %s)\n", record.key(), record.value()));
                logger.info("WRITE RECORD TO HDFS");
                HDFSWriter.write(String.format("%d\t%s\n", record.key(), record.value()));
            }
            consumer.commitAsync();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        runConsumer();
    }
}
