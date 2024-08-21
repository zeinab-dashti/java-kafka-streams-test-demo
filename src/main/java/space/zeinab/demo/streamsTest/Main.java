package space.zeinab.demo.streamsTest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.service.UserTopology;

import java.util.List;
import java.util.Properties;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Properties streamsConfig = AppConfig.getStreamsConfig();
        AppConfig.createTopics(AppConfig.getAdminConfig(),
                List.of(AppConfig.INPUT_TOPIC, AppConfig.OUTPUT_TOPIC, AppConfig.WINDOW_OUTPUT_TOPIC)
        );

        Topology topology = UserTopology.buildTopology();
        //Topology topology = UserActivityTopology.buildTopology();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        try {
            streams.start();
        } catch (Exception e) {
            log.error("Exception in starting the stream : {}", e.getMessage(), e);
        }
    }
}