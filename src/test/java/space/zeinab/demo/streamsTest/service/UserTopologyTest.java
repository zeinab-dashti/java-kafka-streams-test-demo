package space.zeinab.demo.streamsTest.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.model.User;
import space.zeinab.demo.streamsTest.serde.SerdesFactory;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UserTopologyTest {
    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, User> inputTopic;
    TestOutputTopic<String, User> outputTopic;

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(UserTopology.buildTopology());
        inputTopic = topologyTestDriver.createInputTopic(AppConfig.INPUT_TOPIC, Serdes.String().serializer(), SerdesFactory.userSerdes().serializer());
        outputTopic = topologyTestDriver.createOutputTopic(AppConfig.OUTPUT_TOPIC, Serdes.String().deserializer(), SerdesFactory.userSerdes().deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void userTopology_whenSingleValidInput_thenProcess() {
        var user = new User("user1", "user1-name", "user1-address", LocalDateTime.now());
        inputTopic.pipeInput(user.userId(), user);

        var outputMessageCount = outputTopic.getQueueSize();
        var outputMessage = outputTopic.readKeyValue().value.name();

        assertThat(outputMessageCount).isEqualTo(1);
        assertThat(outputMessage).isEqualTo("USER1-NAME");
    }

    @Test
    void userTopology_whenMultipleValidInput_thenProcessAll() {
        var user1 = new User("user1", "user1-name", "user1-address", LocalDateTime.now());
        var user2 = new User("user2", "user2-name", "user2-address", LocalDateTime.now());
        var keyValue1 = KeyValue.pair(user1.userId(), user1);
        var keyValue2 = KeyValue.pair(user2.userId(), user2);
        inputTopic.pipeKeyValueList(List.of(keyValue1, keyValue2));

        var outputMessageCount = outputTopic.getQueueSize();
        var users = outputTopic.readValuesToList();
        var outputMessage1 = users.get(0).name();
        var outputMessage2 = users.get(1).name();

        assertThat(outputMessageCount).isEqualTo(2);
        assertThat(outputMessage1).isEqualTo("USER1-NAME");
        assertThat(outputMessage2).isEqualTo("USER2-NAME");
    }

    @Test
    void userTopology_whenInvalidInput_thenProcessAndFilter() {
        var user = new User("user1", "user1-name", "Invalid address", LocalDateTime.now());
        inputTopic.pipeInput(user.userId(), user);

        var isFiltered = outputTopic.isEmpty();

        assertThat(isFiltered).isTrue();
    }

    @Test
    void getStateStoreItems_whenValidInput_thenPutInStateStore() {
        var user1 = new User("user1", "user1-name", "user1-old address", LocalDateTime.now());
        var user1Update = new User("user1", "user1-name", "user1-new address", LocalDateTime.now());
        var keyValue1 = KeyValue.pair(user1.userId(), user1);
        var keyValue1Update = KeyValue.pair(user1Update.userId(), user1Update);
        inputTopic.pipeKeyValueList(List.of(keyValue1, keyValue1Update));

        KeyValueStore<String, User> store = topologyTestDriver.getKeyValueStore("user-store");
        var outputUser = store.get("user1");

        assertThat(outputUser.address()).isEqualTo("user1-new address");
    }
}