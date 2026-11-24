package com.clnewze.lab.kafka.common;

/**
 * 설비가 보내는 측정 이벤트 한 건.
 *
 * @param eventId    이벤트 고유 번호. 같은 메시지가 두 번 와도 한 번만 처리하는 데 쓴다 (step05)
 * @param deviceId   설비 번호. <b>메시지 키</b>로 쓴다 → 같은 설비의 이벤트는 항상 같은 파티션 → 순서 보장
 * @param type       측정 종류 (TEMPERATURE 등)
 * @param value      측정값
 * @param occurredAt 발생 시각 (epoch millis)
 */
public record DeviceEvent(String eventId, String deviceId, String type, double value, long occurredAt) {
}
