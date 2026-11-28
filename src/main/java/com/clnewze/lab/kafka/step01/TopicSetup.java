package com.clnewze.lab.kafka.step01;

import com.clnewze.lab.kafka.common.KafkaSettings;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Step 01 — 토픽 만들기.
 *
 * <p>토픽은 "메시지를 담는 이름 붙은 로그"이고, 실제로는 파티션 여러 개로 쪼개져 있다.
 * 파티션 수가 곧 <b>동시에 처리할 수 있는 소비자의 최대 수</b>다 (step04 에서 확인).
 *
 * <p>Kafka 클라이언트는 세 종류다.
 * <ul>
 *   <li>{@code Admin} — 토픽 만들기 · 지우기 · 조회 같은 관리 작업 ← 이 단계</li>
 *   <li>{@code KafkaProducer} — 보내기 (step02)</li>
 *   <li>{@code KafkaConsumer} — 받기 (step03)</li>
 * </ul>
 *
 * <pre>./gradlew topic</pre>
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *   ★TopicSetup (Admin) ──createTopics──▶ Kafka
 *                                          ├ device-events      (파티션 3) ← step02~05 가 쓴다
 *                                          └ device-events.DLT  (파티션 1) ← step05 가 실패분을 넣는다
 * </pre>
 * <ul>
 *   <li><b>역할</b>: 나머지 모든 단계가 쓸 토픽을 미리 준비한다. 브로커가 토픽 자동 생성을 꺼 두었기 때문에(docker-compose) <b>가장 먼저</b> 실행해야 한다</li>
 *   <li><b>입력 / 출력</b>: 없음 / 토픽 2개</li>
 *   <li><b>쓰는 것</b>: {@code KafkaSettings}</li>
 *   <li><b>다음 단계</b>: step02 SimpleProducer 가 이 토픽에 보낸다</li>
 * </ul>
 */
public final class TopicSetup {

    private static final Logger log = LoggerFactory.getLogger(TopicSetup.class);

    public static void main(String[] args) throws Exception {
        // Kafka 클라이언트 설정은 전부 Properties(키-값)로 넘긴다.
        // 꼭 필요한 건 bootstrap.servers 하나 — "처음 접속할 브로커 주소".
        // 클러스터의 나머지 브로커 주소는 첫 접속에서 알아서 받아 온다.
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());

        // 엉뚱한 브로커(다른 프로젝트의 Kafka)에 토픽을 만들지 않도록 주소부터 보여 준다
        log.info("브로커 {} 에 연결", KafkaSettings.bootstrapServers());

        // Admin 은 네트워크 연결 · 스레드를 잡고 있으므로 다 쓰면 닫아야 한다 → try-with-resources
        try (Admin admin = Admin.create(props)) {
            // 실습에서 쓸 토픽 두 개. 본 토픽은 파티션 3개, 실패 메시지용 DLT 는 1개면 충분
            createIfAbsent(admin, KafkaSettings.TOPIC, KafkaSettings.PARTITIONS);
            createIfAbsent(admin, KafkaSettings.DLT_TOPIC, 1);

            // Admin 의 메서드는 모두 비동기다. 결과로 KafkaFuture 를 주고, .get() 으로 끝날 때까지 기다린다.
            // (방금 만든 토픽이 목록에 바로 안 보일 수 있다 — 브로커 메타데이터가 퍼지는 데 잠깐 걸린다)
            log.info("브로커에 있는 토픽: {}", admin.listTopics().names().get());

            // 토픽 상세: 파티션마다 누가 리더인지, 복제본이 몇 개인지
            Map<String, TopicDescription> described =
                    admin.describeTopics(List.of(KafkaSettings.TOPIC, KafkaSettings.DLT_TOPIC)).allTopicNames().get();
            described.values().forEach(TopicSetup::print);
        }
    }

    /**
     * 토픽이 없으면 만들고, 이미 있으면 그냥 넘어간다.
     *
     * <p>"있는지 먼저 확인하고 만들기" 대신 "일단 만들고 이미 있다는 에러를 무시" 하는 방식이다.
     * 확인과 생성 사이에 다른 누가 만들 수도 있으니, 이쪽이 더 안전하다.
     *
     * <p>simulation.FactorySimulation 도 이 메서드로 토픽을 준비한다 → public
     */
    public static void createIfAbsent(Admin admin, String name, int partitions) throws InterruptedException {
        // NewTopic(이름, 파티션 수, 복제 수). 복제 수는 브로커 수보다 클 수 없다 (실습은 브로커 1대 → 1)
        NewTopic topic = new NewTopic(name, partitions, KafkaSettings.REPLICATION_FACTOR);
        try {
            admin.createTopics(List.of(topic)).all().get();
            log.info("토픽 생성: {} (파티션 {})", name, partitions);
        } catch (ExecutionException e) {
            // 비동기 작업의 실패는 ExecutionException 으로 감싸져 온다. 진짜 원인은 getCause()
            if (e.getCause() instanceof TopicExistsException) {
                log.info("이미 있음: {}", name);
            } else {
                throw new IllegalStateException("토픽 생성 실패: " + name, e.getCause());
            }
        }
    }

    private static void print(TopicDescription description) {
        log.info("── {} ──", description.name());
        for (TopicPartitionInfo p : description.partitions()) {
            // leader   : 이 파티션의 읽기 · 쓰기를 맡은 브로커. 프로듀서 · 컨슈머는 리더하고만 이야기한다
            // replicas : 이 파티션의 복사본을 가진 브로커들 (리더 포함)
            // isr      : In-Sync Replicas. 리더를 잘 따라오고 있는 복제본. 리더가 죽으면 여기서 새 리더를 뽑는다
            log.info("  partition={} leader={} replicas={} isr={}",
                    p.partition(), p.leader().id(), p.replicas().size(), p.isr().size());
        }
    }
}
