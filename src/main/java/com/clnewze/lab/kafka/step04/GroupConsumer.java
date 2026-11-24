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
 * <p>Step 03 과 다른 점:
 * <ul>
 *   <li><b>파티션별 offset 커밋</b> — 어느 파티션을 어디까지 처리했는지 직접 들고 있다가 그 값으로 커밋</li>
 *   <li><b>빼앗기기 직전 커밋</b> — 리밸런스로 파티션을 넘겨주기 전에 처리분을 커밋 (onPartitionsRevoked)</li>
 * </ul>
 *
 * <pre>./gradlew group -Pargs="worker-1"
 * ./gradlew group -Pargs="worker-2"     # 다른 터미널
 * ./gradlew produce -Pargs="30"         # 또 다른 터미널</pre>
 */
public final class GroupConsumer {

    private static final Logger log = LoggerFactory.getLogger(GroupConsumer.class);

    /** 모든 worker 가 같은 그룹 이름을 쓴다 → 파티션을 나눠 갖는다 */
    private static final String GROUP_ID = "step04";

    public static void main(String[] args) {
        // 로그에서 누가 누군지 구분하기 위한 이름. 그룹 동작에는 영향 없다
        String name = KafkaSettings.argOrDefault(args, 0, "worker-" + ProcessHandle.current().pid());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(SimpleConsumer.consumerProps(GROUP_ID));
        SimpleConsumer.installShutdownHook(consumer);

        // 처리는 끝났지만 아직 커밋 안 한 위치 (파티션 → 다음에 읽을 offset).
        // 커밋 값은 "마지막으로 처리한 offset" 이 아니라 "다음에 읽을 offset" 이라 +1 해서 넣는다
        Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();

        // 리밸런스 때 Kafka 가 불러 주는 콜백. 둘 다 poll() 을 부른 스레드(= 메인 스레드) 안에서 호출된다
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 파티션을 뺏기기 직전. 여기서 커밋하지 않으면 새 주인이 마지막 커밋 위치부터 다시 읽어
                // 이미 처리한 메시지를 또 처리한다
                log.info("[{}] 반납 {} → 처리분 커밋", name, partitions);
                commit(consumer, pending);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 새로 맡게 된 파티션. 이 파티션의 커밋 위치 다음부터 읽게 된다
                log.info("[{}] 배정 {} (지금 내 담당 전체: {})", name, partitions, consumer.assignment());
            }
        };

        try (consumer) {
            // 리스너를 같이 넘겨야 리밸런스 순간을 알 수 있다
            consumer.subscribe(List.of(KafkaSettings.TOPIC), listener);
            log.info("[{}] 그룹 '{}' 참가", name, GROUP_ID);

            try {
                while (true) {
                    // 리밸런스는 따로 일어나는 게 아니라 이 poll() 안에서 일어난다.
                    // 그래서 위 리스너 콜백도 poll() 도중에 불린다
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("[{}] partition={} offset={} key={}",
                                name, record.partition(), record.offset(), record.key());

                        // 한 건 처리할 때마다 "이 파티션은 여기까지 했다" 를 갱신
                        pending.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    }
                    commit(consumer, pending);
                }
            } catch (WakeupException e) {
                // 종료 신호. try-with-resources 가 consumer 를 닫기 전에(이 안에서) 마지막 커밋을 한다.
                // (바깥 try 의 catch 였다면 consumer 가 이미 닫힌 뒤라 커밋할 수 없다)
                commit(consumer, pending);
            }
        }
        log.info("[{}] 종료 — 그룹을 나가면 남은 소비자에게 리밸런스가 일어난다", name);
    }

    /** 들고 있던 처리 위치를 브로커에 저장하고 비운다 */
    private static void commit(KafkaConsumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> pending) {
        if (pending.isEmpty()) {
            return;
        }
        // commitSync(map): 지정한 파티션의 지정한 위치만 커밋. 브로커가 저장했다고 답할 때까지 기다린다.
        // (commitAsync 는 안 기다려서 빠르지만, 실패해도 모를 수 있다. 종료 · 반납 직전에는 Sync 가 맞다)
        consumer.commitSync(pending);
        pending.clear();
    }
}
