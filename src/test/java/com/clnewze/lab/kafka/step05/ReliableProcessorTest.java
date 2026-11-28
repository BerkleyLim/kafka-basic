package com.clnewze.lab.kafka.step05;

import com.clnewze.lab.kafka.common.DeviceEvent;
import com.clnewze.lab.kafka.common.Json;
import com.clnewze.lab.kafka.common.KafkaSettings;
import com.clnewze.lab.kafka.step05.ReliableProcessor.Outcome;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 브로커 없이 처리 규칙만 확인한다. MockProducer 는 kafka-clients 에 들어 있다.
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *   이 테스트 ──가짜 record──▶ ReliableProcessor ──DLT 전송──▶ MockProducer (메모리. 보낸 내용을 꺼내 검사)
 * </pre>
 * <ul>
 *   <li><b>확인하는 것</b>: 정상 · 중복 · 깨진 JSON · 이상값 · 일시 실패 후 성공 · 재시도 초과 — step05 의 갈림길 전부</li>
 *   <li><b>왜 브로커 없이 되나</b>: ReliableProcessor 가 Kafka 소비 루프와 분리돼 있고, DLT 용 Producer 를 바깥에서 받기 때문</li>
 *   <li><b>backoff 0</b>: 재시도 대기를 0 으로 주어 테스트가 기다리지 않게 한다</li>
 * </ul>
 */
class ReliableProcessorTest {

    private MockProducer<String, String> dlt;
    private ReliableProcessor processor;
    private long offset;

    @BeforeEach
    void setUp() {
        dlt = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        processor = new ReliableProcessor(dlt, 3, 0);
        offset = 0;
    }

    @Test
    void 정상_메시지는_처리된다() {
        assertEquals(Outcome.PROCESSED, processor.handle(record(event("e1", "TEMPERATURE", 20))));
        assertTrue(dlt.history().isEmpty());
    }

    @Test
    void 같은_eventId_가_다시_오면_건너뛴다() {
        String body = event("e1", "TEMPERATURE", 20);
        processor.handle(record(body));

        assertEquals(Outcome.DUPLICATE_SKIPPED, processor.handle(record(body)));
    }

    @Test
    void 깨진_JSON_은_재시도_없이_DLT_로_간다() {
        assertEquals(Outcome.SENT_TO_DLT, processor.handle(record("{not json")));

        ProducerRecord<String, String> sent = dlt.history().getFirst();
        assertEquals(KafkaSettings.DLT_TOPIC, sent.topic());
        assertEquals("{not json", sent.value());
        assertEquals("0", header(sent, "dlt.original.offset"));
        assertTrue(header(sent, "dlt.reason").startsWith("JSON 아님"));
    }

    @Test
    void 말이_안_되는_값은_DLT_로_간다() {
        assertEquals(Outcome.SENT_TO_DLT, processor.handle(record(event("e2", "TEMPERATURE", -999))));
        assertEquals(1, dlt.history().size());
    }

    @Test
    void 일시적_실패는_재시도해서_성공한다() {
        // FLAKY 는 두 번 실패하고 세 번째에 성공한다. maxAttempts=3 이면 성공
        assertEquals(Outcome.PROCESSED, processor.handle(record(event("e3", "FLAKY", 25))));
        assertTrue(dlt.history().isEmpty());
    }

    @Test
    void 재시도_횟수를_넘기면_DLT_로_간다() {
        ReliableProcessor impatient = new ReliableProcessor(dlt, 2, 0);

        assertEquals(Outcome.SENT_TO_DLT, impatient.handle(record(event("e4", "FLAKY", 25))));
        assertTrue(header(dlt.history().getFirst(), "dlt.reason").startsWith("재시도 2회 실패"));
    }

    private ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>(KafkaSettings.TOPIC, 0, offset++, "device-A", value);
    }

    private static String event(String eventId, String type, double value) {
        return Json.write(new DeviceEvent(eventId, "device-A", type, value, 0L));
    }

    private static String header(ProducerRecord<String, String> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
