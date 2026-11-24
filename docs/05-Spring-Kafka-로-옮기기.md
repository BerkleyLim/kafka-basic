# 05. Spring Kafka 로 옮기기

여기서 직접 한 것이 Spring Kafka 에서는 어디로 가는지 대응표. 개념을 알고 나면 Spring 설정이 "무엇을 대신해 주는지" 보인다.

| 이 저장소에서 직접 한 것 | Spring Kafka |
|---|---|
| `Admin.createTopics` (Step 01) | `@Bean NewTopic` — `KafkaAdmin` 이 시작할 때 만든다 |
| `KafkaProducer` + `producerProps()` | `KafkaTemplate` + `spring.kafka.producer.*` |
| poll 루프 (Step 03) | `@KafkaListener` — 컨테이너가 루프를 돈다 |
| `enable.auto.commit=false` + `commitSync()` | `AckMode` (기본 `BATCH`: poll 한 묶음 처리 후 커밋) |
| `wakeup()` 종료 훅 | 컨테이너 수명 주기가 처리 |
| `ConsumerRebalanceListener` (Step 04) | `ConsumerAwareRebalanceListener` |
| 재시도 + DLT (Step 05) | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` |
| 영구 실패는 재시도 안 함 | `errorHandler.addNotRetryableExceptions(...)` |
| 깨진 JSON | `ErrorHandlingDeserializer` 로 감싸면 역직렬화 실패도 에러 핸들러로 온다 |
| 재시도 토픽 (non-blocking) | `@RetryableTopic` |

## 최소 예

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
    consumer:
      group-id: device-service
      auto-offset-reset: earliest
```

```java
@Bean
NewTopic deviceEvents() {
    return TopicBuilder.name("device-events").partitions(3).replicas(1).build();
}

@Bean
DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template,
            (record, ex) -> new TopicPartition(record.topic() + ".DLT", 0));
    var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(300L, 2));  // 2번 더 시도
    handler.addNotRetryableExceptions(IllegalArgumentException.class);           // 영구 실패
    return handler;
}

@KafkaListener(topics = "device-events")
void listen(ConsumerRecord<String, String> record) {
    // 처리. 예외를 던지면 errorHandler 가 재시도 → DLT
}
```

`DeadLetterPublishingRecoverer` 가 붙이는 헤더(`kafka_dlt-exception-message`, `kafka_dlt-original-offset` …)는 Step 05 에서 직접 붙인 `dlt.*` 헤더와 같은 역할이다.

멱등 처리(중복 건너뛰기)는 Spring 이 해 주지 않는다. Step 05 와 똑같이 직접 해야 한다.
