package com.clnewze.lab.kafka.step03;

import com.clnewze.lab.kafka.common.KafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Step 03 — 메시지 받기.
 *
 * <p>볼 것:
 * <ol>
 *   <li><b>poll 루프</b> — 소비자는 브로커가 밀어 주는 걸 받는 게 아니라 스스로 당겨 온다(pull)</li>
 *   <li><b>offset 커밋</b> — "여기까지 읽었다" 를 브로커에 적어 둔다. 자동 커밋을 끄고 처리 뒤에 직접 커밋한다</li>
 *   <li><b>auto.offset.reset=earliest</b> — 이 그룹이 처음이면 맨 앞부터 읽는다</li>
 * </ol>
 *
 * <p>한 번 끝까지 읽고 끈 다음 다시 켜 보면 아무것도 안 나온다 — 커밋된 offset 다음부터 읽기 때문.
 * 그룹 이름을 바꿔서 켜면 처음부터 다시 나온다.
 *
 * <pre>./gradlew consume                  # 그룹 step03
 * ./gradlew consume -Pargs="other"     # 다른 그룹 → 처음부터</pre>
 */
public final class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {
        String groupId = KafkaSettings.argOrDefault(args, 0, "step03");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId));
        installShutdownHook(consumer);

        try (consumer) {
            consumer.subscribe(List.of(KafkaSettings.TOPIC));
            log.info("그룹 '{}' 로 '{}' 구독 시작 (Ctrl+C 로 종료)", groupId, KafkaSettings.TOPIC);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("받음 partition={} offset={} key={} value={}",
                            record.partition(), record.offset(), record.key(), record.value());
                }
                if (!records.isEmpty()) {
                    // 처리를 다 하고 나서 커밋 → 중간에 죽으면 다시 받는다 (at-least-once)
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // 종료 신호. 정상 흐름이다
        }
        log.info("소비자 종료");
    }

    public static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 자동 커밋은 "처리했는지와 상관없이 시간이 되면" 커밋한다 → 처리 전에 커밋되면 유실. 끄고 직접 한다
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    /**
     * Ctrl+C 를 누르면 poll() 을 깨워(wakeup) WakeupException 을 내게 하고, 메인 스레드가 정리를 끝낼 때까지 기다린다.
     * KafkaConsumer 는 스레드 안전하지 않다 — 다른 스레드에서 부를 수 있는 건 wakeup() 하나뿐.
     */
    public static void installShutdownHook(KafkaConsumer<?, ?> consumer) {
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                main.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
