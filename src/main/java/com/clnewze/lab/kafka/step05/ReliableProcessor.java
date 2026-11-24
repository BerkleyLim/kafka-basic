package com.clnewze.lab.kafka.step05;

import com.clnewze.lab.kafka.common.DeviceEvent;
import com.clnewze.lab.kafka.common.Json;
import com.clnewze.lab.kafka.common.KafkaSettings;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Step 05 — 메시지 한 건을 "믿을 수 있게" 처리하는 규칙.
 *
 * <p>Kafka 의 기본 약속은 <b>at-least-once</b>(적어도 한 번)다. 커밋 전에 죽으면 같은 메시지가 다시 온다.
 * 그래서 소비자는 세 가지를 스스로 챙겨야 한다.
 *
 * <ol>
 *   <li><b>중복</b> — 같은 eventId 가 다시 오면 건너뛴다 (멱등 처리)</li>
 *   <li><b>일시적 실패</b> — 잠깐 기다렸다 다시 해 보면 되는 실패는 몇 번 재시도한다</li>
 *   <li><b>영구적 실패</b> — 본문이 깨졌거나 값이 말이 안 되면 재시도해도 소용없다 → DLT 로 옮기고 다음으로 간다.
 *       안 그러면 이 한 건 때문에 파티션 전체가 멈춘다 (poison pill)</li>
 * </ol>
 *
 * <p>이 클래스는 Kafka 소비 루프와 분리해 두어 {@code MockProducer} 로 단위 테스트할 수 있다.
 */
public final class ReliableProcessor {

    private static final Logger log = LoggerFactory.getLogger(ReliableProcessor.class);

    public enum Outcome { PROCESSED, DUPLICATE_SKIPPED, SENT_TO_DLT }

    /** 재시도하면 될 수도 있는 실패 (DB 순간 끊김, 외부 API 타임아웃 등) */
    static final class TransientFailure extends RuntimeException {
        TransientFailure(String message) {
            super(message);
        }
    }

    /** 재시도해도 안 되는 실패 (데이터 자체가 잘못됨) */
    static final class PermanentFailure extends RuntimeException {
        PermanentFailure(String message) {
            super(message);
        }
    }

    private final Producer<String, String> dltProducer;
    private final int maxAttempts;
    private final long backoffMillis;

    /**
     * 처리 끝난 eventId. <b>실습용 메모리 보관</b>이라 재시작하면 잊는다.
     * 실무에서는 처리 결과와 같은 DB 트랜잭션 안에서 eventId 에 유일 제약을 건 표에 넣는다.
     */
    private final Set<String> processedEventIds = new HashSet<>();

    /** FLAKY 이벤트를 몇 번째 시도 중인지 (일시적 실패 흉내용) */
    private final Map<String, Integer> attemptsByEventId = new HashMap<>();

    public ReliableProcessor(Producer<String, String> dltProducer, int maxAttempts, long backoffMillis) {
        this.dltProducer = dltProducer;
        this.maxAttempts = maxAttempts;
        this.backoffMillis = backoffMillis;
    }

    public Outcome handle(ConsumerRecord<String, String> record) {
        DeviceEvent event;
        try {
            event = Json.read(record.value());
        } catch (JsonProcessingException e) {
            return sendToDlt(record, "JSON 아님: " + e.getOriginalMessage());
        }

        if (processedEventIds.contains(event.eventId())) {
            log.info("중복 건너뜀 eventId={} (offset={})", event.eventId(), record.offset());
            return Outcome.DUPLICATE_SKIPPED;
        }

        for (int attempt = 1; ; attempt++) {
            try {
                apply(event);
                processedEventIds.add(event.eventId());
                return Outcome.PROCESSED;
            } catch (PermanentFailure e) {
                return sendToDlt(record, e.getMessage());
            } catch (TransientFailure e) {
                if (attempt >= maxAttempts) {
                    return sendToDlt(record, "재시도 " + maxAttempts + "회 실패: " + e.getMessage());
                }
                log.warn("일시적 실패 {}회째 eventId={} → {}ms 뒤 재시도", attempt, event.eventId(), backoffMillis * attempt);
                sleep(backoffMillis * attempt);
            }
        }
    }

    /** 실제 업무 처리 자리. 여기서는 규칙 두 개로 실패를 흉내 낸다 */
    private void apply(DeviceEvent event) {
        if (event.value() < -273.15) {
            throw new PermanentFailure("절대영도보다 낮은 온도: " + event.value());
        }
        if ("FLAKY".equals(event.type())) {
            int attempt = attemptsByEventId.merge(event.eventId(), 1, Integer::sum);
            if (attempt < 3) {
                throw new TransientFailure("외부 저장소 응답 없음 (흉내)");
            }
        }
        log.info("처리 완료 device={} {}={}", event.deviceId(), event.type(), event.value());
    }

    /**
     * DLT 로 원본을 그대로 옮기고, 왜 실패했는지와 원래 위치는 헤더에 적는다.
     * 동기(.get())로 보낸다 — DLT 저장이 확인되기 전에 원본 offset 을 커밋하면 메시지가 사라진다.
     */
    private Outcome sendToDlt(ConsumerRecord<String, String> record, String reason) {
        ProducerRecord<String, String> dlt = new ProducerRecord<>(KafkaSettings.DLT_TOPIC, record.key(), record.value());
        dlt.headers()
                .add("dlt.reason", bytes(reason))
                .add("dlt.original.topic", bytes(record.topic()))
                .add("dlt.original.partition", bytes(String.valueOf(record.partition())))
                .add("dlt.original.offset", bytes(String.valueOf(record.offset())));
        try {
            dltProducer.send(dlt).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DLT 전송 중단", e);
        } catch (ExecutionException e) {
            // DLT 에도 못 넣었으면 커밋하면 안 된다 → 예외를 올려 소비 루프를 멈춘다
            throw new IllegalStateException("DLT 전송 실패", e.getCause());
        }
        log.warn("DLT 로 보냄 offset={} 이유={}", record.offset(), reason);
        return Outcome.SENT_TO_DLT;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
