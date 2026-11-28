package com.clnewze.lab.kafka.roundtrip;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 진짜 Kafka(Docker)로 "같은 키는 같은 파티션, 파티션 안에서는 순서 유지" 를 확인한다.
 * Docker 가 켜져 있어야 한다.
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *   Testcontainers ──띄움──▶ 임시 Kafka 컨테이너 (실습용 docker-compose 와 별개, 테스트 끝나면 사라짐)
 *   이 테스트: Producer ──send──▶ 토픽 roundtrip(파티션 3) ──poll──▶ Consumer ── 키별 순서 비교
 * </pre>
 * <ul>
 *   <li><b>확인하는 것</b>: step02 에서 눈으로 본 "키 = 파티션 고정, 파티션 안 순서 유지" 를 자동으로</li>
 *   <li><b>실습 브로커와의 관계</b>: 손대지 않는다. docker-compose 로 띄운 Kafka 가 꺼져 있어도 돌아간다 (Docker 자체만 필요)</li>
 * </ul>
 */
class KafkaRoundTripIT {

    private static final String TOPIC = "roundtrip";
    // 느린 PC · CI 에서도 뜰 수 있게 기동 대기를 넉넉히 (기본 60초)
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0")
            .withStartupTimeout(Duration.ofMinutes(3));

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
    }

    @AfterAll
    static void stop() {
        KAFKA.stop();
    }

    @Test
    void 같은_키는_같은_파티션에_보낸_순서대로_쌓인다() throws Exception {
        List<String> keys = List.of("device-A", "device-B", "device-C");
        int perKey = 20;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < perKey; i++) {
                for (String key : keys) {
                    producer.send(new ProducerRecord<>(TOPIC, key, key + "#" + i));
                }
            }
            producer.flush();
        }

        List<ConsumerRecord<String, String>> received = consumeAll(keys.size() * perKey);

        Map<String, Set<Integer>> partitionsByKey = new HashMap<>();
        Map<String, List<String>> valuesByKey = new HashMap<>();
        for (ConsumerRecord<String, String> r : received) {
            partitionsByKey.computeIfAbsent(r.key(), k -> new HashSet<>()).add(r.partition());
            valuesByKey.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(r.value());
        }

        for (String key : keys) {
            assertEquals(1, partitionsByKey.get(key).size(), key + " 는 한 파티션에만 있어야 한다");
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < perKey; i++) {
                expected.add(key + "#" + i);
            }
            assertEquals(expected, valuesByKey.get(key), key + " 의 순서가 보낸 순서와 같아야 한다");
        }
    }

    private static List<ConsumerRecord<String, String>> consumeAll(int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "roundtrip-test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 30_000;
            while (received.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
            }
        }
        assertEquals(expected, received.size(), "보낸 만큼 받아야 한다");
        return received;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }
}
