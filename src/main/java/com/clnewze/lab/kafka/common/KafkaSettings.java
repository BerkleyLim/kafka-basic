package com.clnewze.lab.kafka.common;

/**
 * 실습 전체가 같이 쓰는 값.
 *
 * <p>브로커 주소 정하는 순서
 * <ol>
 *   <li>{@code KAFKA_BOOTSTRAP} 이 있으면 그대로 (예: {@code broker1:9092,broker2:9092})</li>
 *   <li>{@code KAFKA_PORT} 가 있으면 {@code localhost:<그 값>} — docker-compose 와 같은 변수</li>
 *   <li>둘 다 없으면 {@code localhost:9092}</li>
 * </ol>
 */
public final class KafkaSettings {

    /** 설비 이벤트가 흐르는 토픽. 파티션 3개 */
    public static final String TOPIC = "device-events";

    /** 처리에 실패한 메시지를 따로 모아 두는 토픽 (Dead Letter Topic) */
    public static final String DLT_TOPIC = "device-events.DLT";

    public static final int PARTITIONS = 3;

    /** 브로커가 1대라 복제 수도 1. 운영은 보통 3 */
    public static final short REPLICATION_FACTOR = 1;

    private KafkaSettings() {
    }

    public static String bootstrapServers() {
        String bootstrap = System.getenv("KAFKA_BOOTSTRAP");
        if (bootstrap != null && !bootstrap.isBlank()) {
            return bootstrap;
        }
        String port = System.getenv("KAFKA_PORT");
        return "localhost:" + ((port == null || port.isBlank()) ? "9092" : port);
    }

    /** 실행 인자 첫 번째 값, 없으면 기본값 */
    public static String argOrDefault(String[] args, int index, String defaultValue) {
        return (args != null && args.length > index && !args[index].isBlank()) ? args[index] : defaultValue;
    }
}
