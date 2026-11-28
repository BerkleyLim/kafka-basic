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
 * <p>프로듀서 안에서 일어나는 일:
 * <pre>
 * send(record)
 *   → 직렬화 (key, value 를 byte[] 로)
 *   → 파티션 결정 (키가 있으면 hash(key) % 파티션 수)
 *   → 파티션별 버퍼에 쌓기
 *   → I/O 스레드가 묶어서 브로커로 전송 (linger.ms 만큼 모았다가)
 *   → 브로커 응답 → 콜백 호출
 * </pre>
 *
 * <pre>./gradlew produce -Pargs="12"</pre>
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *   ★SimpleProducer ──send(key=deviceId)──▶ device-events ──▶ step03 · 04 · 05 소비자
 * </pre>
 * <ul>
 *   <li><b>역할</b>: 공장 설비(device-A/B/C) 흉내. 정상 이벤트만 보낸다</li>
 *   <li><b>입력 / 출력</b>: 보낼 건수(-Pargs) / {@code device-events} 토픽</li>
 *   <li><b>쓰는 것</b>: {@code KafkaSettings}, {@code DeviceEvent}, {@code Json}</li>
 *   <li><b>가져다 쓰는 곳</b>: {@link #producerProps()} 를 step05 MixedEventProducer 와 ReliableConsumer(DLT 전송용)가 재사용</li>
 *   <li><b>다음 단계</b>: step03 SimpleConsumer 로 받아 본다</li>
 * </ul>
 */
public final class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

    /** 설비 세 대. 이 값이 메시지 키가 된다 */
    private static final List<String> DEVICES = List.of("device-A", "device-B", "device-C");

    public static void main(String[] args) throws Exception {
        int count = Integer.parseInt(KafkaSettings.argOrDefault(args, 0, "9"));

        // KafkaProducer 는 스레드 안전하다. 실무에서는 앱 전체에서 하나 만들어 같이 쓴다 (매번 만들면 느리다)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < count; i++) {
                // device-A, B, C 를 돌아가며
                String deviceId = DEVICES.get(i % DEVICES.size());

                // 보낼 내용. eventId 는 이벤트마다 새로 만든다 → step05 에서 중복을 가려내는 데 쓴다
                DeviceEvent event = new DeviceEvent(
                        UUID.randomUUID().toString(),
                        deviceId,
                        "TEMPERATURE",
                        Math.round(ThreadLocalRandom.current().nextDouble(18, 32) * 10) / 10.0,
                        System.currentTimeMillis());

                // ProducerRecord(토픽, 키, 값). 파티션을 직접 지정하지 않으면 키로 정해진다.
                // 키를 null 로 주면 프로듀서가 알아서 흩뿌린다 → 순서 보장 없음
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(KafkaSettings.TOPIC, deviceId, Json.write(event));

                // 비동기 전송: send() 는 버퍼에 넣고 바로 돌아온다. 결과는 콜백으로.
                // 콜백은 프로듀서의 I/O 스레드에서 돈다 → 여기서 오래 걸리는 일을 하면 전송 전체가 느려진다
                producer.send(record, (RecordMetadata meta, Exception error) -> {
                    if (error != null) {
                        // 재시도(retries)를 다 써도 안 되면 여기로 온다. 실무에서는 알람 · 재처리 대상
                        log.error("전송 실패 key={}", deviceId, error);
                        return;
                    }
                    // meta 에 "어느 파티션의 몇 번(offset)에 저장됐는지" 가 들어 있다
                    log.info("보냄 key={} → partition={} offset={}", deviceId, meta.partition(), meta.offset());
                });

                // 참고: 하나씩 확실히 보내고 싶으면 producer.send(record).get() — 동기 전송. 대신 느리다
            }
            // 버퍼에 남은 걸 다 보낼 때까지 기다린다. close() 도 하지만, 명시해 두면 읽기 쉽다
            producer.flush();
        }
        log.info("{}건 전송 끝", count);
    }

    /** 다른 단계(step05)에서도 같은 설정을 쓰도록 public 으로 둔다 */
    public static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());

        // 키 · 값을 byte[] 로 바꾸는 방법. Kafka 는 내용이 뭔지 모르고 byte[] 로만 저장한다.
        // 받는 쪽은 같은 방식(StringDeserializer)으로 되돌려야 한다 — 이게 보내는 쪽과 받는 쪽의 약속
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks — 브로커가 언제 "받았다" 고 답할지
        //   0   : 답을 안 기다림. 가장 빠르고 가장 잘 잃는다
        //   1   : 리더만 저장하면 OK. 리더가 바로 죽으면 잃을 수 있다
        //   all : 리더 + 따라오는 복제본(ISR) 모두 저장해야 OK. 가장 안전 (Kafka 3.0+ 기본값)
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 멱등 프로듀서: 응답을 못 받아 다시 보냈는데 사실 첫 번째도 저장됐던 경우,
        // 브로커가 (프로듀서 ID, 순번)을 보고 두 번째를 버린다 → 재전송으로 인한 중복 · 순서 꼬임 방지
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 묶어서 보내기: 최대 5ms 기다렸다가 모인 걸 한 번에 → 네트워크 왕복이 줄어 처리량 ↑ (지연은 조금 ↑)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return props;
    }
}
