import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public interface KafkaConfig {
    String TOPIC = "test_datalab";
    String BROKERS = "localhost:9092"; // a.k.a BOOTSTRAP_SERVERS
    String PRODUCER_CLIENT_ID = "ApacheLogProducer";
    String GROUP_ID_0 = "apachelog";
    Class<LongSerializer> KEY_SERIALIZER = LongSerializer.class;
    Class<StringSerializer> VALUE_SERIALIZER = StringSerializer.class;
    Class<LongDeserializer> KEY_DESERIALIZER = LongDeserializer.class;
    Class<StringDeserializer> VALUE_DESERIALIZER = StringDeserializer.class;
}
