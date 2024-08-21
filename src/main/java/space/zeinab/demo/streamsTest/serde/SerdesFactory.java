package space.zeinab.demo.streamsTest.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import space.zeinab.demo.streamsTest.model.User;

public class SerdesFactory {
    static public Serde<User> userSerdes() {
        JsonSerializer<User> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<User> jsonDeserializer = new JsonDeserializer<>(User.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}