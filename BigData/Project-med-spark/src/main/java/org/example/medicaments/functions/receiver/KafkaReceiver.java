package org.example.medicaments.functions.receiver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.parser.TextToMedicamentFunc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@RequiredArgsConstructor
@Slf4j
public class KafkaReceiver implements Supplier<JavaDStream<Medicament>> {
    private final List<String> topics;
    private final JavaStreamingContext jsc;
//    private final String servers = "";
//    private final String groupId = "";
//    private final String autoReset = "";

    Config config = ConfigFactory.load("application.conf");
    String servers = config.getString("app.kafka.bootstrap.servers");
    String groupId = config.getString("app.kafka.group.id");
    String autoReset = config.getString("app.kafka.auto.offset.reset");

    private final Map<String, Object> kakfaParms = new HashMap<String, Object>() {{
        put("bootstrap.servers", servers);
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id", groupId);
        put("auto.offset.reset", autoReset);
    }};

    private final TextToMedicamentFunc textToMedicamentFunc = new TextToMedicamentFunc();
    private final Function<String, Medicament> mapper = textToMedicamentFunc::apply;

    @Override
    public JavaDStream<Medicament> get() {
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kakfaParms)
        );
        JavaDStream<Medicament> javaDStream = directStream.map(ConsumerRecord::value).map(mapper);
        return javaDStream;
    }
}
