# 02. CLI 실습 — 코드 없이 먼저 만져 보기

Kafka 이미지 안에 콘솔 도구가 들어 있다. **컨테이너 안에서는 `kafka:19092` 로 붙는다** ([01-개념 §7](01-개념.md#7-리스너와-advertisedlisteners--가장-흔한-접속-문제)).

```bash
docker compose up -d
alias k='docker exec -it kafka-basic'
B='--bootstrap-server kafka:19092'
```

## 토픽

```bash
k /opt/kafka/bin/kafka-topics.sh $B --create --topic cli-test --partitions 3 --replication-factor 1
k /opt/kafka/bin/kafka-topics.sh $B --list
k /opt/kafka/bin/kafka-topics.sh $B --describe --topic cli-test
```

## 보내기 — 키와 함께

```bash
k /opt/kafka/bin/kafka-console-producer.sh $B --topic cli-test \
  --reader-property parse.key=true --reader-property key.separator=:
> device-A:hello
> device-A:world
> device-B:bye
# Ctrl+D 로 끝
```

## 받기

```bash
# 처음부터, 키 · 파티션 · offset 같이 보기
k /opt/kafka/bin/kafka-console-consumer.sh $B --topic cli-test --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

> Kafka 4.x 에서 `--property` 는 `--reader-property`(producer) / `--formatter-property`(consumer) 로 바뀌는 중이다. 옛 이름도 아직 되지만 경고가 나온다.

## 소비자 그룹

```bash
# 그룹으로 읽기 (터미널 두 개에서 같은 그룹으로 띄워 보기)
k /opt/kafka/bin/kafka-console-consumer.sh $B --topic cli-test --group cli-group

# 그룹 상태 — 파티션별 커밋 위치와 밀린 양(LAG)
k /opt/kafka/bin/kafka-consumer-groups.sh $B --describe --group cli-group

# 그룹의 위치를 맨 앞으로 되돌리기 (그룹에 소비자가 없을 때만 된다)
k /opt/kafka/bin/kafka-consumer-groups.sh $B --group cli-group --topic cli-test \
  --reset-offsets --to-earliest --execute
```

`--describe` 출력 읽는 법:

```
GROUP   TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
step05  device-events  0          8               8               0
```

- `CURRENT-OFFSET` : 그룹이 커밋한 위치 (다음에 읽을 번호)
- `LOG-END-OFFSET` : 파티션 끝
- `LAG` : 아직 안 읽은 개수. **운영에서 가장 먼저 보는 숫자**

## 웹 UI

```bash
docker compose --profile ui up -d   # http://localhost:8989
```

토픽 · 메시지 · 그룹 · LAG 를 화면으로 본다. CLI 로 한 번 해 본 뒤 보면 무엇을 보여 주는지 바로 보인다.
