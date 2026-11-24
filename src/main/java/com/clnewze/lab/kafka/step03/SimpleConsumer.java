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
        // 그룹 이름. 같은 이름끼리는 파티션을 나눠 갖고, 읽은 위치(offset)도 그룹 단위로 저장된다
        String groupId = KafkaSettings.argOrDefault(args, 0, "step03");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(groupId));
        installShutdownHook(consumer);

        try (consumer) {
            // subscribe: "이 토픽을 읽겠다". 어느 파티션을 맡을지는 그룹 코디네이터(브로커)가 정해 준다.
            // (직접 파티션을 고르는 assign() 도 있지만, 그러면 그룹 기능 — 나눠 갖기 · 리밸런스 — 을 못 쓴다)
            consumer.subscribe(List.of(KafkaSettings.TOPIC));
            log.info("그룹 '{}' 로 '{}' 구독 시작 (Ctrl+C 로 종료)", groupId, KafkaSettings.TOPIC);

            while (true) {
                // poll: 새 메시지를 가져온다. 없으면 최대 1초 기다렸다가 빈 결과를 준다.
                // poll 은 메시지를 가져오는 것 말고도 리밸런스 참여 · "아직 처리 중" 신호를 같이 한다.
                // → poll 사이 간격이 max.poll.interval.ms(기본 5분)를 넘으면 죽은 걸로 보고 그룹에서 뺀다
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    // 받은 메시지에는 어디서 왔는지(파티션 · offset)와 키 · 값이 들어 있다
                    log.info("받음 partition={} offset={} key={} value={}",
                            record.partition(), record.offset(), record.key(), record.value());
                }

                if (!records.isEmpty()) {
                    // 처리를 다 하고 나서 커밋 → 중간에 죽으면 커밋 안 된 것들을 다시 받는다 (at-least-once).
                    // commitSync() 는 "방금 poll 로 받은 것 전부의 다음 위치" 를 파티션별로 저장한다
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // 종료 훅에서 wakeup() 을 부르면 poll() 이 이 예외를 던진다. 에러가 아니라 정상 종료 신호다.
            // try-with-resources 가 consumer.close() 를 불러 그룹에서 깔끔하게 나간다
        }
        log.info("소비자 종료");
    }

    /** 다른 단계(step04, step05)에서도 같은 설정을 쓰도록 public 으로 둔다 */
    public static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 보낸 쪽(StringSerializer)과 짝이 맞아야 한다
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 이 그룹의 커밋 기록이 없을 때 어디서부터 읽을지
        //   earliest : 남아 있는 것 중 맨 앞부터
        //   latest   : 지금부터 들어오는 것만 (기본값)
        // 커밋 기록이 있으면 이 값은 무시되고 커밋 위치부터 읽는다
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 자동 커밋은 "처리했는지와 상관없이 시간이 되면(기본 5초마다)" 커밋한다
        // → 처리 전에 커밋되고 그 뒤 죽으면 그 메시지는 영영 처리 안 된다(유실). 끄고 직접 한다
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    /**
     * Ctrl+C 를 누르면 poll() 을 깨워(wakeup) WakeupException 을 내게 하고, 메인 스레드가 정리를 끝낼 때까지 기다린다.
     *
     * <p>KafkaConsumer 는 스레드 안전하지 않다 — 다른 스레드에서 불러도 되는 건 wakeup() 하나뿐.
     * 그래서 종료 훅(다른 스레드)에서 close() 를 직접 부르지 않고, 메인 스레드가 닫게 한다.
     *
     * <p>close() 를 하고 나가면 그룹이 바로 리밸런스한다. 그냥 죽으면 브로커가 session.timeout.ms(기본 45초)
     * 동안 기다린 뒤에야 죽은 걸 알아채고, 그동안 이 소비자의 파티션은 아무도 안 읽는다.
     */
    public static void installShutdownHook(KafkaConsumer<?, ?> consumer) {
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                // 메인 스레드가 마지막 커밋 · close 를 끝낼 때까지 최대 10초 기다린다.
                // 종료 훅이 끝나면 JVM 이 바로 내려가므로, 안 기다리면 close 도중에 끊긴다
                main.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
