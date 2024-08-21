package space.zeinab.demo.streamsTest.config;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import space.zeinab.demo.streamsTest.serde.SerdesFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@UtilityClass
public class AppConfig {
    public final String INPUT_TOPIC = "input_topic";
    public final String BOOTSTRAP_SERVERS = "localhost:9092";
    public final String OUTPUT_TOPIC = "output_topic";
    public final String WINDOW_OUTPUT_TOPIC = "windowed_output_topic";

    public Properties getStreamsConfig() {
        Properties streamConfig = new Properties();
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SerdesFactory.class);
        return streamConfig;
    }

    public Properties getAdminConfig() {
        Properties adminConfig = new Properties();
        adminConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return adminConfig;
    }

    public Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public void createTopics(Properties config, List<String> topics) {
        AdminClient admin = AdminClient.create(config);
        int partitions = 1;
        short replication = 1;

        List<NewTopic> newTopics = topics
                .stream()
                .map(topic -> new NewTopic(topic, partitions, replication))
                .collect(Collectors.toList());

        CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult.all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ", e.getMessage(), e);
        }
    }
}