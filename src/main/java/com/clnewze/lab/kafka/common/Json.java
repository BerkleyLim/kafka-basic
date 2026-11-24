package com.clnewze.lab.kafka.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 메시지 본문 직렬화.
 *
 * <p>Kafka 는 본문을 그냥 byte[] 로 본다. 무엇으로 바꿀지는 생산자와 소비자의 약속이다.
 * 이 실습은 String(JSON) 으로 보낸다 — 콘솔 도구로 눈으로 보기 쉽기 때문.
 */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }

    public static String write(DeviceEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("JSON 변환 실패: " + event, e);
        }
    }

    /** 본문이 JSON 이 아니면 {@link JsonProcessingException} — 다시 시도해도 고쳐지지 않는 실패다 */
    public static DeviceEvent read(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, DeviceEvent.class);
    }
}
