package com.clnewze.lab.kafka.step05;

import com.clnewze.lab.kafka.common.KafkaSettings;
import com.clnewze.lab.kafka.step02.SimpleProducer;
import com.clnewze.lab.kafka.step03.SimpleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Step 05 — 중복 · 재시도 · DLT 를 갖춘 소비자.
 *
 * <pre>./gradlew produceBad     # 정상 · 중복 · 깨진 JSON · 이상값 · 일시 실패 섞어 보내기
 * ./gradlew reliable</pre>
 *
 * DLT 에 무엇이 쌓였는지는 콘솔 도구로 헤더까지 볼 수 있다 → docs/05-신뢰성.md
 */
public final class ReliableConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReliableConsumer.class);

    public static void main(String[] args) {
        String groupId = KafkaSettings.argOrDefault(args, 0, "step05");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(SimpleConsumer.consumerProps(groupId));
        SimpleConsumer.installShutdownHook(consumer);
        Map<ReliableProcessor.Outcome, Integer> stats = new EnumMap<>(ReliableProcessor.Outcome.class);

        try (consumer; KafkaProducer<String, String> dltProducer = new KafkaProducer<>(SimpleProducer.producerProps())) {
            ReliableProcessor processor = new ReliableProcessor(dltProducer, 3, 300);
            consumer.subscribe(List.of(KafkaSettings.TOPIC));
            log.info("그룹 '{}' 시작 (Ctrl+C 로 종료)", groupId);

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        stats.merge(processor.handle(record), 1, Integer::sum);
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync();
                        log.info("누적 결과 {}", stats);
                    }
                }
            } catch (WakeupException e) {
                // 종료 신호
            }
        }
        log.info("종료. 최종 결과 {}", stats);
    }
}
