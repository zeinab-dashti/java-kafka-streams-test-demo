package space.zeinab.demo.streamsTest.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.model.User;
import space.zeinab.demo.streamsTest.serde.SerdesFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UserActivityTopologyTest {
    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, User> inputTopic;
    TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(UserActivityTopology.buildTopology());
        inputTopic = topologyTestDriver.createInputTopic(AppConfig.INPUT_TOPIC, Serdes.String().serializer(), SerdesFactory.userSerdes().serializer());
        outputTopic = topologyTestDriver.createOutputTopic(AppConfig.WINDOW_OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void userActivityTopology_whenProcess_thenCorrectWindowCreated() {
        var user = new User("user1", "user1-name", "user1-old address", LocalDateTime.parse("2030-01-01T21:21:20"));
        var userUpdate1 = new User("user1", "user1-name", "user1-new address", LocalDateTime.parse("2030-01-01T21:21:22"));
        var userUpdate2 = new User("user1", "user1-name", "user1-new address2", LocalDateTime.parse("2030-01-01T21:21:29"));
        var keyValue = KeyValue.pair(user.userId(), user);
        var keyValueUpdate1 = KeyValue.pair(userUpdate1.userId(), userUpdate1);
        var keyValueUpdate2 = KeyValue.pair(userUpdate2.userId(), userUpdate2);
        inputTopic.pipeKeyValueList(List.of(keyValue, keyValueUpdate1, keyValueUpdate2));

        WindowStore<String, Long> store = topologyTestDriver.getWindowStore("activity-store");
        store.all().forEachRemaining(windowedStoreItems -> {
            var windowStartTime = LocalDateTime.ofInstant(windowedStoreItems.key.window().startTime(), ZoneId.of("UTC"));
            var windowEndTime = LocalDateTime.ofInstant(windowedStoreItems.key.window().endTime(), ZoneId.of("UTC"));

            var expectedStartTime = LocalDateTime.parse("2030-01-01T21:21:15");
            var expectedEndTime = LocalDateTime.parse("2030-01-01T21:21:30");
            var windowedStoreItemsCount = windowedStoreItems.value;

            assertThat(windowedStoreItemsCount).isEqualTo(3);
            assertThat(windowStartTime).isEqualTo(expectedStartTime);
            assertThat(windowEndTime).isEqualTo(expectedEndTime);
        });
    }
}