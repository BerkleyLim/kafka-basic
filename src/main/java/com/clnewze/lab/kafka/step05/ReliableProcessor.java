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
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *   ReliableConsumer ──record──▶ ★handle()
 *                                  ① JSON 읽기 ── 실패 ──────────────────────▶ DLT
 *                                  ② 중복 검사 ── 이미 처리 ─▶ DUPLICATE_SKIPPED
 *                                  ③ 처리 ─┬ 성공 ─────────▶ PROCESSED
 *                                          ├ 영구 실패 ────────────────────────▶ DLT
 *                                          └ 일시 실패 ─ 재시도 ─ 초과 ────────▶ DLT
 * </pre>
 * <ul>
 *   <li><b>역할</b>: 업무 규칙. Kafka 소비 루프를 모른다 — record 하나 받고 결과 하나 돌려줄 뿐</li>
 *   <li><b>입력 / 출력</b>: {@code ConsumerRecord} 한 건 / {@link Outcome}, 실패 시 {@code device-events.DLT} 에 원본 + 이유 헤더</li>
 *   <li><b>쓰는 것</b>: {@code Json}, {@code DeviceEvent}, {@code KafkaSettings}, DLT 용 {@code Producer}(바깥에서 받는다 → 테스트에서 MockProducer 로 바꿔 끼운다)</li>
 *   <li><b>쓰는 곳</b>: ReliableConsumer, ReliableProcessorTest</li>
 * </ul>
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

    /**
     * 메시지 한 건을 처리하고 결과를 돌려준다. 이 메서드가 정상적으로 돌아오면 소비 루프는 커밋해도 된다.
     * 예외를 던지는 경우는 DLT 저장마저 실패했을 때뿐이다 → 그때는 커밋하면 안 된다.
     */
    public Outcome handle(ConsumerRecord<String, String> record) {
        // ① 본문 읽기. Kafka 는 내용을 검사하지 않으므로 아무 문자열이나 올 수 있다
        DeviceEvent event;
        try {
            event = Json.read(record.value());
        } catch (JsonProcessingException e) {
            return sendToDlt(record, "JSON 아님: " + e.getOriginalMessage());
        }

        // ② 중복 검사. at-least-once 라서 같은 메시지가 다시 오는 건 "언젠가 반드시" 일어난다.
        //    (커밋 전에 죽음, 리밸런스, 프로듀서 쪽 재전송 등)
        if (processedEventIds.contains(event.eventId())) {
            log.info("중복 건너뜀 eventId={} (offset={})", event.eventId(), record.offset());
            return Outcome.DUPLICATE_SKIPPED;
        }

        // ③ 처리 + 재시도. 실패 종류에 따라 갈린다
        //    PermanentFailure → 바로 DLT (다시 해도 결과가 같다)
        //    TransientFailure → 잠깐 쉬고 다시. maxAttempts 를 넘기면 DLT
        //
        //    주의: 여기서 쉬는 동안 이 파티션의 다음 메시지는 기다린다(blocking retry).
        //    재시도를 길게 잡으면 poll 간격이 max.poll.interval.ms 를 넘어 그룹에서 쫓겨날 수 있다
        for (int attempt = 1; ; attempt++) {
            try {
                apply(event);
                // 처리에 성공한 뒤에 기록한다. 기록 먼저 하면, 처리 도중 죽었을 때 다시 와도 건너뛰게 된다
                processedEventIds.add(event.eventId());
                return Outcome.PROCESSED;
            } catch (PermanentFailure e) {
                return sendToDlt(record, e.getMessage());
            } catch (TransientFailure e) {
                if (attempt >= maxAttempts) {
                    return sendToDlt(record, "재시도 " + maxAttempts + "회 실패: " + e.getMessage());
                }
                log.warn("일시적 실패 {}회째 eventId={} → {}ms 뒤 재시도", attempt, event.eventId(), backoffMillis * attempt);
                // 대기 시간을 조금씩 늘린다 (300ms → 600ms …). 상대가 회복할 틈을 준다
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
        // 원본의 키 · 값을 그대로 옮긴다. 고쳐서 보내지 않는다 — 나중에 원인을 보려면 원본이 필요하다
        ProducerRecord<String, String> dlt = new ProducerRecord<>(KafkaSettings.DLT_TOPIC, record.key(), record.value());
        // 헤더: 본문과 별개로 붙이는 키-값 메타데이터. "왜 · 어디서" 를 여기 적어 두면
        // DLT 를 보는 사람이 원본 토픽을 뒤지지 않아도 된다
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
