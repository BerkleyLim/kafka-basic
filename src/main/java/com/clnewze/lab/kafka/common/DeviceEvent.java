package com.clnewze.lab.kafka.common;

/**
 * 설비가 보내는 측정 이벤트 한 건.
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *   Producer: DeviceEvent ─Json.write─▶ "JSON 문자열" ─▶ Kafka ─▶ "JSON 문자열" ─Json.read─▶ DeviceEvent :Consumer
 * </pre>
 * <ul>
 *   <li><b>역할</b>: 보내는 쪽과 받는 쪽이 공유하는 <b>메시지 계약</b>. 필드를 바꾸면 양쪽이 함께 바뀌어야 한다</li>
 *   <li><b>만드는 곳</b>: step02 SimpleProducer, step05 MixedEventProducer</li>
 *   <li><b>읽는 곳</b>: step05 ReliableProcessor (step03 · 04 는 문자열 그대로 출력만 한다)</li>
 * </ul>
 * 전체 그림: docs/00-아키텍처와-구성.md §3
 *
 * @param eventId    이벤트 고유 번호. 같은 메시지가 두 번 와도 한 번만 처리하는 데 쓴다 (step05)
 * @param deviceId   설비 번호. <b>메시지 키</b>로 쓴다 → 같은 설비의 이벤트는 항상 같은 파티션 → 순서 보장
 * @param type       측정 종류 (TEMPERATURE 등)
 * @param value      측정값
 * @param occurredAt 발생 시각 (epoch millis)
 */
public record DeviceEvent(String eventId, String deviceId, String type, double value, long occurredAt) {
}
