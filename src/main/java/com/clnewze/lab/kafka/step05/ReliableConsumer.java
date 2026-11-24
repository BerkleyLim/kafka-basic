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
        // 결과별 건수 (처리 / 중복 / DLT) — 실행 끝에 한눈에 보려고
        Map<ReliableProcessor.Outcome, Integer> stats = new EnumMap<>(ReliableProcessor.Outcome.class);

        // 소비자와 DLT 용 프로듀서를 같이 연다. try 에 둘 다 넣으면 끝날 때 역순으로 둘 다 닫힌다

        try (consumer; KafkaProducer<String, String> dltProducer = new KafkaProducer<>(SimpleProducer.producerProps())) {
            // 최대 3번 시도, 재시도 간격 300ms 부터 시작
            ReliableProcessor processor = new ReliableProcessor(dltProducer, 3, 300);
            consumer.subscribe(List.of(KafkaSettings.TOPIC));
            log.info("그룹 '{}' 시작 (Ctrl+C 로 종료)", groupId);

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        // 한 건씩 처리. 실패해도 DLT 로 옮기고 정상적으로 돌아오므로 루프는 멈추지 않는다.
                        // DLT 저장마저 실패하면 예외가 올라와 루프를 빠져나간다 → 아래 커밋을 안 하게 된다
                        stats.merge(processor.handle(record), 1, Integer::sum);
                    }
                    if (!records.isEmpty()) {
                        // 묶음 전체를 처리(또는 DLT 로 이동)한 뒤에만 커밋
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
