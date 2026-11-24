package com.clnewze.lab.kafka.step02;

import com.clnewze.lab.kafka.common.DeviceEvent;
import com.clnewze.lab.kafka.common.Json;
import com.clnewze.lab.kafka.common.KafkaSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Step 02 — 메시지 보내기.
 *
 * <p>볼 것 세 가지:
 * <ol>
 *   <li><b>키</b> — 같은 키(deviceId)는 항상 같은 파티션으로 간다. 출력에서 device-A 의 partition 이 늘 같은지 본다</li>
 *   <li><b>offset</b> — 파티션 안에서 1씩 늘어나는 번호. 파티션이 다르면 offset 도 따로 센다</li>
 *   <li><b>비동기</b> — send() 는 바로 돌아오고, 결과는 콜백으로 나중에 온다</li>
 * </ol>
 *
 * <pre>./gradlew produce -Pargs="12"</pre>
 */
public final class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);
    private static final List<String> DEVICES = List.of("device-A", "device-B", "device-C");

    public static void main(String[] args) throws Exception {
        int count = Integer.parseInt(KafkaSettings.argOrDefault(args, 0, "9"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < count; i++) {
                String deviceId = DEVICES.get(i % DEVICES.size());
                DeviceEvent event = new DeviceEvent(
                        UUID.randomUUID().toString(),
                        deviceId,
                        "TEMPERATURE",
                        Math.round(ThreadLocalRandom.current().nextDouble(18, 32) * 10) / 10.0,
                        System.currentTimeMillis());

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(KafkaSettings.TOPIC, deviceId, Json.write(event));

                // 비동기 전송: 결과는 콜백으로. 콜백은 producer 의 I/O 스레드에서 돈다 → 무거운 일 금지
                producer.send(record, (RecordMetadata meta, Exception error) -> {
                    if (error != null) {
                        log.error("전송 실패 key={}", deviceId, error);
                        return;
                    }
                    log.info("보냄 key={} → partition={} offset={}", deviceId, meta.partition(), meta.offset());
                });
            }
            // 버퍼에 남은 걸 다 보낼 때까지 기다린다. close() 도 하지만, 명시해 두면 읽기 쉽다
            producer.flush();
        }
        log.info("{}건 전송 끝", count);
    }

    public static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all : 리더 + 따라오는 복제본(ISR) 모두가 받아야 성공. 유실을 가장 덜 하는 설정
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 멱등 프로듀서 : 재전송 때문에 같은 메시지가 두 번 쓰이는 걸 브로커가 막아 준다 (Kafka 3.0+ 기본값 true)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 묶어서 보내기 : 최대 5ms 기다렸다가 모아서 한 번에 → 처리량 ↑
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return props;
    }
}
