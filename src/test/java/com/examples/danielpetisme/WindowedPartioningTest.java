package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@Testcontainers
public class WindowedPartioningTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(WindowedPartioningTest.class);

    public static final String CLIENT_ID = WindowedPartioningTest.class.getName();
    public static final String STATE_STORE = "counter";

    public static final String CHANGELOG_TOPIC = String.format("%s-%s-changelog", CLIENT_ID, STATE_STORE);

    static final String IputTopic = "in";
    static final String OutputTopic = "out";

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    Topology createTopology() {

        final var builder = new StreamsBuilder();

        builder.stream(IputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> LOGGER.info("Read record: " + k + " => " + v))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(10)))
                .count(Materialized.as(STATE_STORE))
                .toStream()
                .peek((Windowed<String> key, Long value) -> LOGGER.info("Windowed record: key " + key + " => " + value))
                .map((Windowed<String> key, Long value) -> new KeyValue<>(key.toString(), value.toString()))
                .peek((k, v) -> LOGGER.info("Produced record: key " + k + " => " + v))
                .to(OutputTopic,
                        Produced.<String, String>as("aggregated-value")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        return builder.build();
    }

    final List<TestRecord<String, String>> input = List.of(
            new TestRecord("abc", "1111", null, 1L),
            new TestRecord("abc", "2222", null, 2L),
            new TestRecord("bcd", "1111", null, 3L),
            new TestRecord("abc", "3333", null, 21L),
            new TestRecord("abc", "4444", null, 31L),
            new TestRecord("bcd", "5555", null, 41L)
    );

    final List<TestRecord<String, String>> expectedRecords = List.of(
            new TestRecord("[abc@0/10]", "1", null, 1L),
            new TestRecord("[abc@0/10]", "2", null, 3L),
            new TestRecord("[bcd@0/10]", "1", null, 3L),
            new TestRecord("[abc@20/30]", "1", null, 21L),
            new TestRecord("[abc@30/40]", "1", null, 31L),
            new TestRecord("[bcd@40/50]", "1", null, 41L)
    );

    @Test
    public void runTestContainer() throws Exception {
        //Produce Data and run Kafka Streams
        createTopics(List.of(IputTopic, OutputTopic), 3, 1);
        Map<String, Integer> inputKeyPartition = produceData(input); //Capturing the key and partition
        KafkaStreamsExecutionRecords kafkaStreamsExecutionRecords = runKafkaStreams();

        // Verifying the output topic
        List<ConsumerRecord<String, String>> outputTopicRecords = kafkaStreamsExecutionRecords.outputTopicRecords;
        assertThat(outputTopicRecords.size()).isEqualTo(expectedRecords.size());

        Map<String, Integer> outputKeyPartition = new HashMap<>();
        for (ConsumerRecord<String, String> record : outputTopicRecords) {
            // Applying substring to extract the original key from the string representation of the compound key
            outputKeyPartition.put(record.key().substring(1,4), record.partition());
        }
        assertThat(outputKeyPartition).isNotEqualTo(inputKeyPartition);


        // Verifying the changelog data is partitioned
        List<ConsumerRecord<Windowed<String>, Long>> changelogRecords = kafkaStreamsExecutionRecords.changelogRecords;
        assertThat(changelogRecords.size()).isEqualTo(expectedRecords.size());


        Map<String, Integer> changelogKeyPartition = new HashMap<>();
        for (ConsumerRecord<Windowed<String>, Long> record : changelogRecords) {
            changelogKeyPartition.put(record.key().key(), record.partition());
//            LOGGER.info(
//                    "Changelog: (Key: {}, Partition: {}) - Input (Key: {}, Partition: {}))",
//                    record.key(), record.partition(),
//                    record.key().key(), inputKeyPartition.get(record.key().key())
//            );
        }
        assertThat(changelogKeyPartition).isEqualTo(inputKeyPartition);
    }

    private KafkaStreamsExecutionRecords runKafkaStreams() {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT_ID);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, Paths.get("target", "testcontainers", UUID.randomUUID().toString()).toString());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);


        Topology topology = createTopology();
        LOGGER.info(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(createTopology(), streamsConfig);
        streams.setUncaughtExceptionHandler((Throwable t) -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

        List<ConsumerRecord<String, String>> outputRecords = consumeData(expectedRecords.size(), 10_000);

        //In this context, consumption is faster than the compaction, as such # of message sent to the changelog topic == # of input messages
        List<ConsumerRecord<Windowed<String>, Long>> changelogRecords = consumeChangelogData(expectedRecords.size(), 10_000);

        streams.close(Duration.ofSeconds(3));

        return new KafkaStreamsExecutionRecords(outputRecords, changelogRecords);
    }


    private static List<ConsumerRecord<String, String>> consumeData(int expectedRecords, int timeoutMs) {
        KafkaConsumer<String, String> outputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID + "-output-consumer-test",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer());
        outputConsumer.subscribe(Collections.singletonList(OutputTopic));


        List<ConsumerRecord<String, String>> loaded = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (loaded.size() < expectedRecords && System.currentTimeMillis() - start < timeoutMs) {
            ConsumerRecords<String, String> records = outputConsumer.poll(Duration.of(3, ChronoUnit.SECONDS));
            records.forEach((record) -> loaded.add(record));
        }

        outputConsumer.close(Duration.ofSeconds(3));

        return loaded;
    }

    private static List<ConsumerRecord<Windowed<String>, Long>> consumeChangelogData(int expectedRecords, int timeoutMs) {
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), 10L);
        windowedDeserializer.setIsChangelogTopic(true);

        KafkaConsumer<Windowed<String>, Long> changelogConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID + "-changelog-consumer-test",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ), windowedDeserializer, new LongDeserializer());
        changelogConsumer.subscribe(Collections.singletonList(CHANGELOG_TOPIC));


        List<ConsumerRecord<Windowed<String>, Long>> loaded = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (loaded.size() < expectedRecords && System.currentTimeMillis() - start < timeoutMs) {
            ConsumerRecords<Windowed<String>, Long> records = changelogConsumer.poll(Duration.of(3, ChronoUnit.SECONDS));
            records.forEach((record) -> loaded.add(record));
        }

        changelogConsumer.close(Duration.ofSeconds(3));
        return loaded;
    }

    private static Map<String, Integer> produceData(List<TestRecord<String, String>> input) {
        Map<String, Integer> partitionPerRecordKey = new HashMap<>();

        KafkaProducer<String, String> inputProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, "windowed-key-producer-" + UUID.randomUUID(),
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
                ),
                new StringSerializer(), new StringSerializer()
        );

        input.forEach((record) -> {
            try {
                inputProducer.send(
                        new ProducerRecord<>(IputTopic, null, record.timestamp(), record.key(), record.value()),
                        (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                fail(exception.getMessage());
                            } else {
                                LOGGER.info("--> Sent data, k: {} - v: {}", record.key(), record.value());
                                partitionPerRecordKey.put(record.key(), metadata.partition());
                            }
                        }
                ).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        inputProducer.close(Duration.ofSeconds(3));

        return partitionPerRecordKey;
    }

    private static void createTopics(List<String> topics, int partitions, int rf) throws InterruptedException, ExecutionException {
        var adminClient = AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ));

        // Create topic with 3 partitions
        for (String topicName : topics) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                LOGGER.info("Creating topic {}", topicName);
                final NewTopic newTopic = new NewTopic(topicName, partitions, (short) rf);
                try {
                    CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                    topicsCreationResult.all().get();
                } catch (Exception e) {
                    //silent ignore if topic already exists
                }
            }
        }
    }

    private static class KafkaStreamsExecutionRecords {

        public final List<ConsumerRecord<String, String>> outputTopicRecords;
        public final List<ConsumerRecord<Windowed<String>, Long>> changelogRecords;

        public KafkaStreamsExecutionRecords(List<ConsumerRecord<String, String>> outputTopicRecords, List<ConsumerRecord<Windowed<String>, Long>> changelogRecords) {
            this.outputTopicRecords = outputTopicRecords;
            this.changelogRecords = changelogRecords;
        }
    }
}
