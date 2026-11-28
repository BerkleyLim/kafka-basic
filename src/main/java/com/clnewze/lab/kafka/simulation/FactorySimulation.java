package com.clnewze.lab.kafka.simulation;

import com.clnewze.lab.kafka.common.DeviceEvent;
import com.clnewze.lab.kafka.common.Json;
import com.clnewze.lab.kafka.common.KafkaSettings;
import com.clnewze.lab.kafka.step01.TopicSetup;
import com.clnewze.lab.kafka.step02.SimpleProducer;
import com.clnewze.lab.kafka.step03.SimpleConsumer;
import com.clnewze.lab.kafka.step05.ReliableProcessor;
import com.clnewze.lab.kafka.step05.ReliableProcessor.Outcome;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 시뮬레이션 — Step 01 ~ 05 를 <b>main 하나</b>로 한 번에 돌려 본다.
 *
 * <p>단계별 예제는 터미널을 여러 개 띄워야 전체가 보인다. 이 클래스는 같은 일을 한 프로세스 안에서
 * 스레드 여러 개로 흉내 내고, 끝에 "보낸 것 · 받은 것 · DLT 로 간 것" 을 표로 맞춰 본다.
 *
 * <h2>아키텍처에서의 위치</h2>
 * <pre>
 *                                   ┌─▶ worker-1 ─┐
 *   설비 5대 (main 스레드)          ├─▶ worker-2 ─┼─ 실패분 ─▶ device-events.DLT ─▶ dlt-watcher
 *   ──send──▶ device-events (P3) ──┤  └▶ worker-3 ─┘ (중간에 멈춤 → 리밸런스)
 *                                   │   ↑ 그룹 sim-service-* : 파티션을 <b>나눠</b> 받는다 (Step 04 · 05)
 *                                   └─▶ monitor     그룹 sim-monitor-* : <b>전부</b> 받는다 (그룹이 다르면 각자 다)
 * </pre>
 *
 * <h2>흐름</h2>
 * <ol>
 *   <li>토픽 준비 (Step 01)</li>
 *   <li>소비자 5개 시작 — 처리 서비스 3 · 모니터 1 · DLT 감시 1. 각자 자기 스레드 (KafkaConsumer 는 스레드 안전하지 않다)</li>
 *   <li>파티션 배정이 끝날 때까지 기다린 뒤, 설비 이벤트를 보낸다. 20% 는 일부러 나쁜 메시지 (중복 · 깨진 JSON · 이상값 · 일시 실패 각 5%) (Step 02 · 05)</li>
 *   <li>절반쯤 지나면 worker-3 을 멈춘다 → 남은 둘이 파티션을 넘겨받는다 (Step 04)</li>
 *   <li>다 보내고, 다 받을 때까지 기다린 뒤 결과 요약</li>
 * </ol>
 *
 * <pre>./gradlew simulate                  # 20초
 * ./gradlew simulate -Pargs="40"      # 40초</pre>
 *
 * <p>그룹 이름에 실행 시각을 붙여 매번 새 그룹으로 시작하고, 그룹이 처음이면 <b>지금부터</b>(latest) 읽는다.
 * 그래서 앞 단계에서 쌓아 둔 메시지와 섞이지 않는다.
 */
public final class FactorySimulation {

    private static final Logger log = LoggerFactory.getLogger(FactorySimulation.class);

    private static final List<String> DEVICES = List.of("device-A", "device-B", "device-C", "device-D", "device-E");

    /** 보내는 메시지 종류. 비율은 {@link #pickKind()} */
    enum Kind { NORMAL, DUPLICATE, BROKEN_JSON, IMPOSSIBLE_VALUE, FLAKY }

    private FactorySimulation() {
    }

    public static void main(String[] args) throws Exception {
        int seconds = Integer.parseInt(KafkaSettings.argOrDefault(args, 0, "20"));
        String runId = String.valueOf(System.currentTimeMillis());
        log.info("브로커 {} 에 연결 · {}초 동안 실행", KafkaSettings.bootstrapServers(), seconds);

        // ── ① 토픽 준비 (Step 01) ───────────────────────────────
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        try (Admin admin = Admin.create(adminProps)) {
            TopicSetup.createIfAbsent(admin, KafkaSettings.TOPIC, KafkaSettings.PARTITIONS);
            TopicSetup.createIfAbsent(admin, KafkaSettings.DLT_TOPIC, 1);
        }

        // DLT 로 보낼 프로듀서는 처리 서비스 3개가 <b>같이</b> 쓴다. KafkaProducer 는 스레드 안전하다
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(SimpleProducer.producerProps())) {

            // ── ② 소비자 시작 ──────────────────────────────────
            String serviceGroup = "sim-service-" + runId;
            List<Worker> services = List.of(
                    Worker.service("worker-1", serviceGroup, producer),
                    Worker.service("worker-2", serviceGroup, producer),
                    Worker.service("worker-3", serviceGroup, producer));
            Worker monitor = Worker.monitor("monitor", "sim-monitor-" + runId);
            Worker dltWatcher = Worker.dltWatcher("dlt-watcher", "sim-dlt-" + runId);

            List<Worker> all = new ArrayList<>(services);
            all.add(monitor);
            all.add(dltWatcher);
            List<Thread> threads = new ArrayList<>();
            for (Worker w : all) {
                // 스레드 이름 = 로그의 [ ] 안에 찍히는 이름 → 누가 한 일인지 로그로 구분된다
                Thread t = new Thread(w, w.name);
                t.start();
                threads.add(t);
            }

            // 파티션 배정이 다 끝나기 전에 보내면, 아직 아무도 안 맡은 파티션의 메시지를 놓칠 수 있다 (latest 로 시작하므로)
            log.info("파티션 배정 기다리는 중 … (그룹 가입 · 리밸런스는 몇 초 걸린다)");
            waitUntil(Duration.ofSeconds(60), () ->
                    services.stream().allMatch(w -> w.assigned.size() == 1)
                            && monitor.assigned.size() == KafkaSettings.PARTITIONS
                            && dltWatcher.assigned.size() == 1);
            services.forEach(w -> log.info("배정 완료 {} → partition {}", w.name, new TreeSet<>(w.partitions())));

            // ── ③ 설비 이벤트 보내기 (Step 02 · 05) ─────────────────
            Map<Kind, AtomicInteger> sent = new EnumMap<>(Kind.class);
            for (Kind k : Kind.values()) {
                sent.put(k, new AtomicInteger());
            }
            Map<String, DeviceEvent> lastNormal = new HashMap<>();

            long start = System.currentTimeMillis();
            long end = start + seconds * 1000L;
            long half = start + seconds * 500L;
            long nextReport = start + 5000;
            boolean crashed = false;
            int i = 0;

            while (System.currentTimeMillis() < end) {
                String device = DEVICES.get(i++ % DEVICES.size());
                Kind kind = pickKind();
                String value = body(kind, device, lastNormal);
                if (value == null) { // 중복을 보낼 차례인데 아직 이 설비의 정상 이벤트가 없으면 정상으로
                    kind = Kind.NORMAL;
                    value = body(kind, device, lastNormal);
                }
                Kind k = kind;
                // 비동기 전송. 성공 콜백에서만 센다 → 요약의 "보낸 수" 는 브로커가 저장을 확인한 수
                producer.send(new ProducerRecord<>(KafkaSettings.TOPIC, device, value), (meta, error) -> {
                    if (error == null) {
                        sent.get(k).incrementAndGet();
                    } else {
                        log.error("전송 실패 {}", device, error);
                    }
                });

                // ── ④ 절반쯤에서 worker-3 장애 흉내 (Step 04) ─────────
                if (!crashed && System.currentTimeMillis() >= half) {
                    crashed = true;
                    Worker victim = services.get(2);
                    log.warn("▼▼ {} 를 멈춘다 (partition {} 담당) → 남은 worker 가 넘겨받는지 보자 ▼▼",
                            victim.name, new TreeSet<>(victim.partitions()));
                    victim.stop();
                }
                if (System.currentTimeMillis() >= nextReport) {
                    nextReport += 5000;
                    log.info("── 진행: 보냄 {} · 처리 서비스가 받음 {} · 모니터가 받음 {} · DLT {}",
                            total(sent), services.stream().mapToInt(Worker::received).sum(),
                            monitor.received(), dltWatcher.received());
                }
                Thread.sleep(150);
            }
            producer.flush();
            int sentTotal = total(sent);
            log.info("보내기 끝 ({}건). 소비자가 따라잡을 때까지 기다린다", sentTotal);

            // ── ⑤ 다 받을 때까지 기다렸다가 정리 ─────────────────────
            waitUntil(Duration.ofSeconds(20), () ->
                    services.stream().mapToInt(Worker::received).sum() >= sentTotal
                            && monitor.received() >= sentTotal);
            Thread.sleep(1000); // DLT 감시가 마지막 DLT 를 받을 틈
            all.forEach(Worker::stop);
            for (Thread t : threads) {
                t.join();
            }
            printSummary(sent, services, monitor, dltWatcher);
        }
    }

    // ── 메시지 만들기 ────────────────────────────────────────────

    /** 정상 80%, 나쁜 메시지 네 종류 각 5% */
    private static Kind pickKind() {
        int r = ThreadLocalRandom.current().nextInt(100);
        if (r < 80) return Kind.NORMAL;
        if (r < 85) return Kind.DUPLICATE;
        if (r < 90) return Kind.BROKEN_JSON;
        if (r < 95) return Kind.IMPOSSIBLE_VALUE;
        return Kind.FLAKY;
    }

    private static String body(Kind kind, String device, Map<String, DeviceEvent> lastNormal) {
        long now = System.currentTimeMillis();
        double temp = Math.round(ThreadLocalRandom.current().nextDouble(18, 32) * 10) / 10.0;
        return switch (kind) {
            case NORMAL -> {
                DeviceEvent e = new DeviceEvent(UUID.randomUUID().toString(), device, "TEMPERATURE", temp, now);
                lastNormal.put(device, e);
                yield Json.write(e);
            }
            // 같은 eventId 를 다시 보낸다 = 프로듀서 재전송 · 소비자 재시작 등으로 "같은 걸 두 번 받는" 상황
            case DUPLICATE -> lastNormal.containsKey(device) ? Json.write(lastNormal.get(device)) : null;
            case BROKEN_JSON -> "{broken json from " + device;
            case IMPOSSIBLE_VALUE -> Json.write(new DeviceEvent(UUID.randomUUID().toString(), device, "TEMPERATURE", -999, now));
            // ReliableProcessor 가 두 번 실패한 척하고 세 번째에 성공한다
            case FLAKY -> Json.write(new DeviceEvent(UUID.randomUUID().toString(), device, "FLAKY", temp, now));
        };
    }

    // ── 결과 요약 ────────────────────────────────────────────────

    private static void printSummary(Map<Kind, AtomicInteger> sent, List<Worker> services, Worker monitor, Worker dlt) {
        int sentTotal = total(sent);
        int expectedDlt = sent.get(Kind.BROKEN_JSON).get() + sent.get(Kind.IMPOSSIBLE_VALUE).get();

        StringBuilder sb = new StringBuilder("\n");
        sb.append("══════════════════════ 시뮬레이션 결과 ══════════════════════\n");
        sb.append(String.format("보냄   총 %d  (정상 %d · 중복 %d · 깨진 JSON %d · 이상값 %d · 일시 실패 %d)%n",
                sentTotal, sent.get(Kind.NORMAL).get(), sent.get(Kind.DUPLICATE).get(),
                sent.get(Kind.BROKEN_JSON).get(), sent.get(Kind.IMPOSSIBLE_VALUE).get(), sent.get(Kind.FLAKY).get()));

        sb.append("\n[처리 서비스] 같은 그룹 3명이 파티션을 나눠 받음\n");
        Map<Outcome, Integer> sum = new EnumMap<>(Outcome.class);
        for (Worker w : services) {
            sb.append(String.format("  %-9s 맡았던 partition %-10s 처리 %3d · 중복 건너뜀 %2d · DLT %2d%s%n",
                    w.name, w.partitionsEverHandled, w.count(Outcome.PROCESSED), w.count(Outcome.DUPLICATE_SKIPPED),
                    w.count(Outcome.SENT_TO_DLT), w == services.get(2) ? "   ← 중간에 멈춤" : ""));
            for (Outcome o : Outcome.values()) {
                sum.merge(o, w.count(o), Integer::sum);
            }
        }
        int serviceReceived = services.stream().mapToInt(Worker::received).sum();
        sb.append(String.format("  합계      받음 %d (처리 %d · 중복 %d · DLT %d)%n", serviceReceived,
                sum.get(Outcome.PROCESSED), sum.get(Outcome.DUPLICATE_SKIPPED), sum.get(Outcome.SENT_TO_DLT)));

        sb.append(String.format("%n[모니터] 다른 그룹이라 처리 서비스와 상관없이 전부 받음: %d%n", monitor.received()));
        sb.append("  설비별 ").append(new TreeMap<>(monitor.byKey)).append('\n');

        sb.append(String.format("%n[DLT 감시] device-events.DLT 에 쌓인 것: %d%n", dlt.received()));
        dlt.dltReasons.forEach((reason, n) -> sb.append(String.format("  %3d건  %s%n", n.get(), reason)));

        sb.append("\n[검증]\n");
        sb.append(check(monitor.received() == sentTotal, "모니터가 받은 수 = 보낸 수",
                "그룹이 다르면 같은 메시지를 각자 다 받는다"));
        sb.append(check(serviceReceived >= sentTotal, "처리 서비스가 받은 수 ≥ 보낸 수 (" + serviceReceived + " ≥ " + sentTotal + ")",
                serviceReceived > sentTotal
                        ? "더 많다 → worker-3 이 커밋하기 전에 멈춰서 그 묶음이 다른 worker 에게 다시 갔다 (at-least-once)"
                        : "worker 하나가 멈췄지만 하나도 잃지 않았다"));
        sb.append(check(dlt.received() == sum.get(Outcome.SENT_TO_DLT), "DLT 감시가 본 수 = 처리 서비스가 DLT 로 보낸 수", null));
        sb.append(check(sum.get(Outcome.SENT_TO_DLT) >= expectedDlt,
                "DLT ≥ 깨진 JSON + 이상값 (" + sum.get(Outcome.SENT_TO_DLT) + " ≥ " + expectedDlt + ")",
                "일시 실패는 재시도로 살아나서 DLT 에 안 간다"));
        sb.append("═══════════════════════════════════════════════════════════\n");
        sb.append("참고: 중복 건너뛰기는 worker 메모리에만 기억한다. 리밸런스로 파티션 주인이 바뀌면\n");
        sb.append("      새 주인은 모르므로 중복이 '처리' 로 셀 수 있다 → 실무에서는 DB 에 eventId 를 남긴다 (docs/04-신뢰성.md §4)\n");
        log.info(sb.toString());
    }

    private static String check(boolean ok, String what, String why) {
        return String.format("  %s %s%s%n", ok ? "✔" : "✘", what, why == null ? "" : "\n      → " + why);
    }

    private static int total(Map<Kind, AtomicInteger> sent) {
        return sent.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    private static void waitUntil(Duration timeout, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                log.warn("{}초 안에 조건을 못 채웠다 — 지금 상태로 계속한다", timeout.toSeconds());
                return;
            }
            Thread.sleep(200);
        }
    }

    // ── 소비자 하나 = 스레드 하나 ─────────────────────────────────

    /**
     * 소비자 한 개를 자기 스레드에서 돌린다. 역할은 세 가지.
     * <ul>
     *   <li>service — ReliableProcessor 로 처리 (Step 05)</li>
     *   <li>monitor — 세기만 한다. 다른 그룹이라 처리 서비스와 별개로 전부 받는다</li>
     *   <li>dltWatcher — DLT 토픽을 읽어 헤더의 실패 이유를 모은다</li>
     * </ul>
     * KafkaConsumer 는 스레드 안전하지 않다. 이 스레드만 consumer 를 만지고, 다른 스레드는 {@link #stop()} 의 wakeup() 만 부른다.
     */
    static final class Worker implements Runnable {

        final String name;
        private final String topic;
        private final KafkaConsumer<String, String> consumer;
        private final ReliableProcessor processor; // service 만

        /** 지금 맡은 파티션. 리밸런스 리스너가 고치고, main 스레드가 읽는다 → 동시성 안전한 Set */
        final Set<TopicPartition> assigned = ConcurrentHashMap.newKeySet();
        final Set<Integer> partitionsEverHandled = new java.util.concurrent.ConcurrentSkipListSet<>();
        private final AtomicInteger received = new AtomicInteger();
        private final Map<Outcome, AtomicInteger> outcomes = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> byKey = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> dltReasons = new ConcurrentHashMap<>();

        private Worker(String name, String groupId, String topic, ReliableProcessor processor) {
            this.name = name;
            this.topic = topic;
            this.processor = processor;
            Properties props = SimpleConsumer.consumerProps(groupId);
            // 새 그룹이므로 "지금부터" 읽는다 → 앞 단계에서 쌓인 메시지와 섞이지 않는다
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, name);
            this.consumer = new KafkaConsumer<>(props);
        }

        static Worker service(String name, String groupId, KafkaProducer<String, String> dltProducer) {
            // 최대 3번 시도, 재시도 간격 100ms 부터 (Step 05 와 같은 규칙, 대기만 짧게)
            return new Worker(name, groupId, KafkaSettings.TOPIC, new ReliableProcessor(dltProducer, 3, 100));
        }

        static Worker monitor(String name, String groupId) {
            return new Worker(name, groupId, KafkaSettings.TOPIC, null);
        }

        static Worker dltWatcher(String name, String groupId) {
            return new Worker(name, groupId, KafkaSettings.DLT_TOPIC, null);
        }

        @Override
        public void run() {
            try (consumer) {
                consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        assigned.removeAll(partitions);
                        if (!partitions.isEmpty()) {
                            log.info("반납 {}", partitionNumbers(partitions));
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        if (partitions.isEmpty()) {
                            return;
                        }
                        // 시작 위치를 바로 정하고 커밋해 둔다. 안 그러면 커밋이 없는 파티션이 다른 소비자에게 넘어갈 때
                        // 그 소비자가 다시 latest 로 시작해 그 사이에 들어온 메시지를 건너뛴다
                        Map<TopicPartition, OffsetAndMetadata> start = new HashMap<>();
                        for (TopicPartition tp : partitions) {
                            start.put(tp, new OffsetAndMetadata(consumer.position(tp)));
                        }
                        consumer.commitSync(start);
                        assigned.addAll(partitions);
                        log.info("배정 {} (지금 담당 {})", partitionNumbers(partitions), partitionNumbers(assigned));
                    }
                });

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        handle(record);
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync(); // 묶음을 다 처리한 뒤에만 (Step 05 와 같다)
                    }
                }
            } catch (WakeupException e) {
                // stop() 이 부른 종료 신호. 커밋 안 된 묶음이 있으면 그 그룹의 다른 소비자가 다시 받는다
                log.info("멈춤");
            } catch (RuntimeException e) {
                log.error("오류로 멈춤", e);
            }
        }

        private void handle(ConsumerRecord<String, String> record) {
            received.incrementAndGet();
            partitionsEverHandled.add(record.partition());
            if (processor != null) {
                outcomes.computeIfAbsent(processor.handle(record), o -> new AtomicInteger()).incrementAndGet();
            } else if (topic.equals(KafkaSettings.DLT_TOPIC)) {
                Header reason = record.headers().lastHeader("dlt.reason");
                String text = reason == null ? "(이유 없음)" : new String(reason.value(), StandardCharsets.UTF_8);
                // "JSON 아님: Unexpected character …" 처럼 뒤가 매번 달라지는 부분은 잘라서 묶는다
                String bucket = text.contains(":") ? text.substring(0, text.indexOf(':')) : text;
                dltReasons.computeIfAbsent(bucket, r -> new AtomicInteger()).incrementAndGet();
            } else {
                byKey.computeIfAbsent(record.key(), k -> new AtomicInteger()).incrementAndGet();
            }
        }

        /** 다른 스레드에서 불러도 되는 유일한 consumer 메서드가 wakeup() 이다 */
        void stop() {
            consumer.wakeup();
        }

        int received() {
            return received.get();
        }

        int count(Outcome outcome) {
            AtomicInteger n = outcomes.get(outcome);
            return n == null ? 0 : n.get();
        }

        Set<Integer> partitions() {
            return partitionNumbers(assigned);
        }

        private static Set<Integer> partitionNumbers(Collection<TopicPartition> partitions) {
            Set<Integer> numbers = new TreeSet<>();
            partitions.forEach(tp -> numbers.add(tp.partition()));
            return numbers;
        }
    }
}
