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

        // 같은 객체를 두 번 보내면 eventId 도 같다 → 소비자 입장에서는 "같은 메시지가 두 번 온" 상황
        DeviceEvent normal = new DeviceEvent(UUID.randomUUID().toString(), "device-A", "TEMPERATURE", 21.5, now);
        // 값이 물리적으로 말이 안 된다 → 다시 처리해도 결과가 같다 (영구 실패)
        DeviceEvent impossible = new DeviceEvent(UUID.randomUUID().toString(), "device-B", "TEMPERATURE", -999, now);
        // type=FLAKY 는 처리기가 두 번 실패한 척하고 세 번째에 성공한다 (일시 실패 흉내)
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
        // .get() 으로 한 건씩 기다린다 — 보낸 순서와 결과를 눈으로 맞춰 보기 쉽게 (실습용. 실무는 보통 비동기)
        var meta = producer.send(new ProducerRecord<>(KafkaSettings.TOPIC, key, value)).get();
        log.info("보냄 partition={} offset={} : {}", meta.partition(), meta.offset(), note);
    }
}
