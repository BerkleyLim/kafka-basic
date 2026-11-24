package com.clnewze.lab.kafka.step04;

import com.clnewze.lab.kafka.common.KafkaSettings;
import com.clnewze.lab.kafka.step03.SimpleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Step 04 — 소비자 그룹과 리밸런스.
 *
 * <p>같은 그룹 이름으로 터미널 여러 개에서 띄운다. 파티션이 3개이므로:
 * <ul>
 *   <li>1개 띄우면 → 혼자 0,1,2 를 다 맡는다</li>
 *   <li>2개 → 2개 / 1개로 나눈다</li>
 *   <li>3개 → 하나씩</li>
 *   <li>4개 → 하나는 <b>놀고 있다</b>. 파티션 수보다 소비자를 늘려도 빨라지지 않는다</li>
 * </ul>
 * 하나를 끄면 남은 소비자가 그 파티션을 넘겨받는다(리밸런스). 그 순간이 로그에 찍힌다.
 *
 * <p>추가로 보는 것: <b>파티션별 offset 커밋</b>과 <b>빼앗기기 직전 커밋</b>(onPartitionsRevoked).
 *
 * <pre>./gradlew group -Pargs="worker-1"
 * ./gradlew group -Pargs="worker-2"     # 다른 터미널
 * ./gradlew produce -Pargs="30"         # 또 다른 터미널</pre>
 */
public final class GroupConsumer {

    private static final Logger log = LoggerFactory.getLogger(GroupConsumer.class);
    private static final String GROUP_ID = "step04";

    public static void main(String[] args) {
        String name = KafkaSettings.argOrDefault(args, 0, "worker-" + ProcessHandle.current().pid());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(SimpleConsumer.consumerProps(GROUP_ID));
        SimpleConsumer.installShutdownHook(consumer);

        // 아직 커밋 안 한, 처리 끝난 위치. 커밋 값은 "다음에 읽을 offset" 이라 +1 한다
        Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 파티션을 뺏기기 직전. 여기서 커밋하지 않으면 새 주인이 같은 메시지를 다시 처리한다
                log.info("[{}] 반납 {} → 처리분 커밋", name, partitions);
                commit(consumer, pending);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("[{}] 배정 {} (지금 내 담당 전체: {})", name, partitions, consumer.assignment());
            }
        };

        try (consumer) {
            consumer.subscribe(List.of(KafkaSettings.TOPIC), listener);
            log.info("[{}] 그룹 '{}' 참가", name, GROUP_ID);

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("[{}] partition={} offset={} key={}",
                                name, record.partition(), record.offset(), record.key());
                        pending.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    }
                    commit(consumer, pending);
                }
            } catch (WakeupException e) {
                // 종료 신호. try-with-resources 가 consumer 를 닫기 전에(이 안에서) 마지막 커밋을 한다
                commit(consumer, pending);
            }
        }
        log.info("[{}] 종료 — 그룹을 나가면 남은 소비자에게 리밸런스가 일어난다", name);
    }

    private static void commit(KafkaConsumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> pending) {
        if (pending.isEmpty()) {
            return;
        }
        consumer.commitSync(pending);
        pending.clear();
    }
}
