package space.zeinab.demo.streamsTest.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.model.User;
import space.zeinab.demo.streamsTest.serde.SerdesFactory;

import java.time.Duration;

public class UserActivityTopology {
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, User> inputUser = streamsBuilder.stream(
                AppConfig.INPUT_TOPIC,
                Consumed.with(Serdes.String(), SerdesFactory.userSerdes()).withTimestampExtractor(new UserActivityTimeStampExtractor())
        );
        inputUser.print(Printed.<String, User>toSysOut().withLabel("Input stream for windowed topology"));

        KStream<String, String> windowedUser = inputUser
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(15)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("activity-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        "Key = " + key.key() + ", User Activity Count = " + value
                ));
        windowedUser.print(Printed.<String, String>toSysOut().withLabel("Windowed stream"));

        windowedUser.to(AppConfig.WINDOW_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}