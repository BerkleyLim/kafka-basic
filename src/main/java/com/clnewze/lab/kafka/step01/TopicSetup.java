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
 * <pre>./gradlew topic</pre>
 */
public final class TopicSetup {

    private static final Logger log = LoggerFactory.getLogger(TopicSetup.class);

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());

        // 엉뚱한 브로커(다른 프로젝트의 Kafka)에 토픽을 만들지 않도록 주소부터 보여 준다
        log.info("브로커 {} 에 연결", KafkaSettings.bootstrapServers());

        try (Admin admin = Admin.create(props)) {
            createIfAbsent(admin, KafkaSettings.TOPIC, KafkaSettings.PARTITIONS);
            createIfAbsent(admin, KafkaSettings.DLT_TOPIC, 1);

            log.info("브로커에 있는 토픽: {}", admin.listTopics().names().get());

            Map<String, TopicDescription> described =
                    admin.describeTopics(List.of(KafkaSettings.TOPIC, KafkaSettings.DLT_TOPIC)).allTopicNames().get();
            described.values().forEach(TopicSetup::print);
        }
    }

    static void createIfAbsent(Admin admin, String name, int partitions) throws InterruptedException {
        NewTopic topic = new NewTopic(name, partitions, KafkaSettings.REPLICATION_FACTOR);
        try {
            admin.createTopics(List.of(topic)).all().get();
            log.info("토픽 생성: {} (파티션 {})", name, partitions);
        } catch (ExecutionException e) {
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
            // leader : 이 파티션의 읽기·쓰기를 맡은 브로커 / isr : 리더를 잘 따라오고 있는 복제본
            log.info("  partition={} leader={} replicas={} isr={}",
                    p.partition(), p.leader().id(), p.replicas().size(), p.isr().size());
        }
    }
}
