// com.profect.tickle.domain.event.stream.consumer.TicketEventWorker
package com.profect.tickle.domain.event.stream.consumer;

import com.profect.tickle.domain.event.dto.EventDecision;
import com.profect.tickle.domain.event.service.event.EventCoreLockService;
import com.profect.tickle.domain.event.service.event.PostActionsService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.AutoClaimResult;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.profect.tickle.domain.event.stream.StreamInitializer.GROUP;
import static com.profect.tickle.domain.event.stream.StreamInitializer.STREAM_KEY;
import static com.profect.tickle.domain.point.entity.PointTarget.EVENT;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("redis")
public class TicketEventWorker {

    // ====== 튜닝 파라미터 ======
    private static final String CONSUMER_PREFIX = "worker-";
    private static final int    POOL_SIZE       = 1;       // 동시에 몇 개의 컨슈머 루프를 돌릴지
    private static final int    BATCH           = 16;      // readGroup batch size (최대치)
    private static final long   BLOCK_MS        = 5_000;   // readGroup block timeout
    private static final long   CLAIM_IDLE_MS   = 30_000;  // autoClaim idle 기준
    private static final int    CLAIM_PAGE      = 64;      // autoClaim page size

    /**
     * finite 모드:
     *  - MAX_MESSAGES_PER_WORKER > 0 이면 각 워커는 해당 개수만 처리하고 종료
     *  - 0 이면 무한 컨슈머(기존 동작)
     */
    private static final int    MAX_MESSAGES_PER_WORKER = 0; // ex) 100 으로 두면 각 워커가 100개만 처리하고 끝.

    // ====== 주입 ======
    private final Executor eventExecutor;   // VirtualThreadTaskExecutor("ticket-worker-")
    private final RedissonClient redisson;
    private final EventCoreLockService core;
    private final PostActionsService   postActions;

    @EventListener(ApplicationStartedEvent.class)
    public void start() {
        log.info("[TicketEventWorker] starting... mode={}",
                (MAX_MESSAGES_PER_WORKER > 0 ? "FINITE" : "INFINITE"));
        for (int i = 0; i < POOL_SIZE; i++) {
            final String consumer = CONSUMER_PREFIX + i;
            log.info("[TicketEventWorker] starting consumer {}", consumer);
            if (MAX_MESSAGES_PER_WORKER > 0) {
                eventExecutor.execute(() -> workLoopFinite(consumer, MAX_MESSAGES_PER_WORKER));
            } else {
                eventExecutor.execute(() -> workLoopInfinite(consumer));
            }
        }
        if (MAX_MESSAGES_PER_WORKER == 0) {
            log.info("[TicketEventWorker] starting reclaimer");
            eventExecutor.execute(this::reclaimLoop);
        }
    }

    /** 무한 컨슈머(기존 패턴 유지) */
    private void workLoopInfinite(String consumerName) {
        final long start = System.currentTimeMillis();
        RStream<String, String> stream = redisson.getStream(STREAM_KEY, StringCodec.INSTANCE);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                StreamReadGroupArgs args = StreamReadGroupArgs
                        .greaterThan(StreamMessageId.NEVER_DELIVERED)
                        .count(BATCH)
                        .timeout(Duration.ofMillis(BLOCK_MS));

                Map<StreamMessageId, Map<String, String>> batch =
                        stream.readGroup(GROUP, consumerName, args);

                if (batch == null || batch.isEmpty()) {
                    continue;
                }

                for (var e : batch.entrySet()) {
                    StreamMessageId id = e.getKey();
                    Map<String, String> fields = e.getValue();

                    Long eventId  = asLong(fields.get("eventId"));
                    Long memberId = asLong(fields.get("memberId"));

                    if (eventId == null || memberId == null) {
                        log.error("[{}] bad message fields: {}", consumerName, fields);
                        // ack하지 않음 → autoClaim 대상
                        continue;
                    }

                    try {
                        EventDecision d = core.applyCore(eventId, memberId);
                        postActions.recordPointHistory(memberId, d.perPrice(), EVENT);
                        log.info("[{}] point created for member={}", consumerName, memberId);
                        if (d.winner()) {
                            postActions.reserveSeatAndCreateReservation(d.seatId(), memberId, d.perPrice());
                        }
                        stream.ack(GROUP, id); // 성공 시 ACK
                        log.info("[{}] ack id={} member={}", consumerName, id, memberId);
                    } catch (Exception ex) {
                        // 실패 시 ACK하지 않음 → PEL 남겨 autoClaim 대상
                        log.error("[{}] ticket event failed id={} err={}", consumerName, id, ex.toString(), ex);
                    }
                }
            } catch (Exception e) {
                log.error("[{}] read loop error: {}", consumerName, e.toString(), e);
                sleepQuiet(200);
            }
        }
        long end = System.currentTimeMillis();
        log.error("execution time={}ms", (end - start));
    }

    /** 요청 받은 만큼만 처리하는 finite 컨슈머 */
    private void workLoopFinite(String consumerName, int maxMessages) {
        final long started = System.currentTimeMillis();
        int processed = 0;
        RStream<String, String> stream = redisson.getStream(STREAM_KEY, StringCodec.INSTANCE);

        while (processed < maxMessages && !Thread.currentThread().isInterrupted()) {
            try {
                // 과다 배정 방지: 남은 예산 이하만 읽기
                int want = Math.min(BATCH, maxMessages - processed);

                StreamReadGroupArgs args = StreamReadGroupArgs
                        .greaterThan(StreamMessageId.NEVER_DELIVERED)
                        .count(want)
                        .timeout(Duration.ofMillis(BLOCK_MS));

                Map<StreamMessageId, Map<String, String>> batch =
                        stream.readGroup(GROUP, consumerName, args);

                if (batch == null || batch.isEmpty()) {
                    continue; // 정확히 N개를 채우고 싶다면 계속 대기
                }

                for (var e : batch.entrySet()) {
                    if (processed >= maxMessages) break;

                    StreamMessageId id = e.getKey();
                    Map<String, String> fields = e.getValue();

                    Long eventId  = asLong(fields.get("eventId"));
                    Long memberId = asLong(fields.get("memberId"));

                    if (eventId == null || memberId == null) {
                        log.error("[{}] bad message fields: {}", consumerName, fields);
                        // ack하지 않음 → autoClaim 대상
                        continue;
                    }

                    try {
                        EventDecision d = core.applyCore(eventId, memberId);
                        postActions.recordPointHistory(memberId, d.perPrice(), EVENT);
                        log.info("[{}] point created for member={}", consumerName, memberId);
                        if (d.winner()) {
                            postActions.reserveSeatAndCreateReservation(d.seatId(), memberId, d.perPrice());
                        }
                        stream.ack(GROUP, id);
                        log.info("[{}] ack id={} member={}", consumerName, id, memberId);
                        processed++;
                    } catch (Exception ex) {
                        log.error("[{}] ticket event failed id={} err={}", consumerName, id, ex.toString(), ex);
                        // ACK 안 함 → PEL 보존 (재처리 가능)
                    }
                }
            } catch (Exception e) {
                log.error("[{}] read loop error: {}", consumerName, e.toString(), e);
                sleepQuiet(200);
            }
        }
        log.info("[{}] workLoopFinite finished. processed={} elapsed={}ms",
                consumerName, processed, System.currentTimeMillis() - started);
    }

    private void reclaimLoop() {
        RStream<String, String> stream = redisson.getStream(STREAM_KEY, StringCodec.INSTANCE);
        StreamMessageId start = StreamMessageId.MIN;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                AutoClaimResult<String, String> res = stream.autoClaim(
                        GROUP,
                        CONSUMER_PREFIX + "reclaimer",
                        CLAIM_IDLE_MS, TimeUnit.MILLISECONDS,
                        start,
                        CLAIM_PAGE
                );

                Map<StreamMessageId, Map<String, String>> claimed = res.getMessages();
                start = res.getNextId();

                if (claimed.isEmpty()) {
                    sleepQuiet(2_000);
                    continue;
                }

                for (var e : claimed.entrySet()) {
                    StreamMessageId id = e.getKey();
                    Map<String, String> fields = e.getValue();

                    Long eventId  = asLong(fields.get("eventId"));
                    Long memberId = asLong(fields.get("memberId"));

                    if (eventId == null || memberId == null) {
                        log.error("[reclaimer] bad message fields: {}", fields);
                        continue;
                    }

                    try {
                        EventDecision d = core.applyCore(eventId, memberId);
                        postActions.recordPointHistory(memberId, d.perPrice(), EVENT);
                        if (d.winner()) {
                            postActions.reserveSeatAndCreateReservation(d.seatId(), memberId, d.perPrice());
                        }
                        stream.ack(GROUP, id);
                        log.info("[reclaimer] ack id={} member={}", id, memberId);
                    } catch (Exception ex) {
                        log.error("[reclaimer] reclaim failed id={} err={}", id, ex.toString(), ex);
                    }
                }

            } catch (Exception e) {
                log.warn("[reclaimer] autoClaim error: {}", e.toString(), e);
                sleepQuiet(2_000);
            }
        }
        log.info("[reclaimer] loop finished.");
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping TicketEventWorker...");
        if (eventExecutor instanceof java.util.concurrent.ExecutorService es) {
            es.shutdownNow(); // 블로킹 read는 timeout 뒤 체크 → 그 다음 반복에서 종료
            log.info("TicketEventWorker executor shut down.");
        }
    }

    // utils
    private static Long asLong(Object v) {
        if (v instanceof Long l) return l;
        if (v instanceof Integer i) return i.longValue();
        return (v == null) ? null : Long.valueOf(v.toString());
    }
}