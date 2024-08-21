package space.zeinab.demo.streamsTest.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.model.User;
import space.zeinab.demo.streamsTest.serde.SerdesFactory;

import java.time.LocalDateTime;

@Slf4j
public class UserTopology {
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, User> inputUser = streamsBuilder.stream(AppConfig.INPUT_TOPIC, Consumed.with(Serdes.String(), SerdesFactory.userSerdes()));
        inputUser.print(Printed.<String, User>toSysOut().withLabel("Input stream for user topology"));

        KStream<String, User> modifiedUser = inputUser
                .mapValues(value -> new User(value.userId(), value.name().toUpperCase(), value.address(), LocalDateTime.now()))
                .mapValues(value -> {
                    if (value.address().contains("Invalid address")) {
                        try {
                            throw new IllegalStateException(value.address());
                        } catch (Exception e) {
                            log.error("Exception is userTopology : {} ", value);
                            return null;
                        }
                    }
                    return value;
                })
                .filter((key, value) -> key != null && value != null);

        KTable<String, User> reducedUser = modifiedUser
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.userSerdes()))
                .reduce(
                        (oldValue, newValue) -> newValue,
                        Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("user-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.userSerdes())
                );
        reducedUser.toStream().print(Printed.<String, User>toSysOut().withLabel("Reduced stream"));

        reducedUser.toStream().to(AppConfig.OUTPUT_TOPIC, Produced.with(Serdes.String(), SerdesFactory.userSerdes()));

        return streamsBuilder.build();
    }
}