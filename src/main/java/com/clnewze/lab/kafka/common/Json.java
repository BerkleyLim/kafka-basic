package com.clnewze.lab.kafka.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 메시지 본문 직렬화.
 *
 * <p>Kafka 는 본문을 그냥 byte[] 로 본다. 무엇으로 바꿀지는 생산자와 소비자의 약속이다.
 * 이 실습은 String(JSON) 으로 보낸다 — 콘솔 도구로 눈으로 보기 쉽기 때문.
 *
 * <h2>아키텍처에서의 위치</h2>
 * <ul>
 *   <li><b>역할</b>: {@link DeviceEvent} ⇄ JSON 문자열. 문자열 ⇄ byte[] 는 Kafka 의 StringSerializer 가 따로 한다 (2단 변환)</li>
 *   <li><b>쓰는 곳</b>: 프로듀서(write) — step02, step05 / 소비자(read) — step05 ReliableProcessor</li>
 *   <li><b>실패의 의미</b>: {@code read} 가 실패하면 "본문이 망가진 메시지" → ReliableProcessor 가 재시도 없이 DLT 로 보낸다</li>
 * </ul>
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
