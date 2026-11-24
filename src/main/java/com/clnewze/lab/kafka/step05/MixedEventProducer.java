package com.clnewze.lab.kafka.step05;

import com.clnewze.lab.kafka.common.DeviceEvent;
import com.clnewze.lab.kafka.common.Json;
import com.clnewze.lab.kafka.common.KafkaSettings;
import com.clnewze.lab.kafka.step02.SimpleProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Step 05 용 — 일부러 문제가 섞인 메시지를 보낸다.
 *
 * <pre>./gradlew produceBad</pre>
 */
public final class MixedEventProducer {

    private static final Logger log = LoggerFactory.getLogger(MixedEventProducer.class);

    public static void main(String[] args) throws Exception {
        long now = System.currentTimeMillis();
        DeviceEvent normal = new DeviceEvent(UUID.randomUUID().toString(), "device-A", "TEMPERATURE", 21.5, now);
        DeviceEvent impossible = new DeviceEvent(UUID.randomUUID().toString(), "device-B", "TEMPERATURE", -999, now);
        DeviceEvent flaky = new DeviceEvent(UUID.randomUUID().toString(), "device-C", "FLAKY", 25.0, now);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(SimpleProducer.producerProps())) {
            send(producer, "device-A", Json.write(normal), "정상");
            send(producer, "device-A", Json.write(normal), "중복 (같은 eventId 를 한 번 더) → 건너뜀");
            send(producer, "device-B", "{this is not json", "깨진 JSON → DLT");
            send(producer, "device-B", Json.write(impossible), "말이 안 되는 값 → DLT");
            send(producer, "device-C", Json.write(flaky), "일시 실패 두 번 뒤 성공 → 재시도");
        }
    }

    private static void send(KafkaProducer<String, String> producer, String key, String value, String note)
            throws Exception {
        var meta = producer.send(new ProducerRecord<>(KafkaSettings.TOPIC, key, value)).get();
        log.info("보냄 partition={} offset={} : {}", meta.partition(), meta.offset(), note);
    }
}
