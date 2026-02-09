package com.profect.tickle.domain.notification.service.realtime.consumer;

import com.profect.tickle.domain.notification.dto.MessageConsumerInfo;
import com.profect.tickle.domain.notification.util.constant.NotificationRedisConstants;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
@Slf4j
@Profile("!local")
public class RedisNotificationConsumer implements MessageConsumer {

    private final StreamOperations<String, String, Object> streamOperations;
    private final NotificationMessageHandler messageHandler;
    private final Executor sseExecutor;

    @Value("#{@notificationStreamKey}")
    private String notificationStreamKey;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedMessages = new AtomicLong(0);
    private final AtomicLong failedMessages = new AtomicLong(0);
    private final long startedAt = System.currentTimeMillis();

    private CompletableFuture<Void> consumerTask;

    @PostConstruct
    private void init() {
        initStreamConsumer();
        start();
    }

    private void initStreamConsumer() {
        try {
            streamOperations.createGroup(
                    notificationStreamKey,
                    ReadOffset.from("0-0"),  // 스트림 처음부터 읽기
                    NotificationRedisConstants.CONSUMER_GROUP
            );
//            log.info("Redis Stream 소비자 그룹 생성: stream={}, group={}",
//                    notificationStreamKey, NotificationRedisConstants.CONSUMER_GROUP);
        } catch (Exception e) {
//            log.info("소비자 그룹 생성 시도 결과: {}", e.getMessage());

            // Consumer Group이 이미 존재하는 경우는 정상
            if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
//                log.info("소비자 그룹이 이미 존재함 - 정상 진행");
            } else {
//                log.error("소비자 그룹 생성 실패", e);
            }
        }
    }


    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerTask = CompletableFuture.runAsync(this::pollAndProcess, sseExecutor);
//            log.info("Redis 메시지 소비자 시작: group={}, consumer={}", NotificationRedisConstants.CONSUMER_GROUP, NotificationRedisConstants.SSE_CONSUMER);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (consumerTask != null) {
                consumerTask.cancel(true);
            }
//            log.info("Redis 메시지 소비자 중지: group={}, consumer={}", NotificationRedisConstants.CONSUMER_GROUP, NotificationRedisConstants.SSE_CONSUMER);
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public MessageConsumerInfo getInfo() {
        // Record 클래스를 사용한 간결한 객체 생성
        return new MessageConsumerInfo(
                "redis",
                NotificationRedisConstants.CONSUMER_GROUP,
                NotificationRedisConstants.SSE_CONSUMER,
                notificationStreamKey,
                running.get(),
                processedMessages.get(),
                failedMessages.get(),
                startedAt
        );
    }

    private void pollAndProcess() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, String, Object>> recordList = streamOperations.read(
                        Consumer.from(NotificationRedisConstants.CONSUMER_GROUP, NotificationRedisConstants.SSE_CONSUMER),
                        StreamReadOptions.empty()
                                .count(10)
                                .block(Duration.ofSeconds(2)),
                        StreamOffset.create(notificationStreamKey, ReadOffset.lastConsumed())
                );

                if (recordList != null && !recordList.isEmpty()) {
                    for (MapRecord<String, String, Object> record : recordList) {
                        handleRecord(record);
                    }
                }

            } catch (Exception e) {
                if (running.get()) {
                    log.error("Redis Stream 메시지 소비 중 오류", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

//        log.info("Redis Stream 소비자 폴링 루프 종료: group={}, consumer={}",
//                NotificationRedisConstants.CONSUMER_GROUP, NotificationRedisConstants.SSE_CONSUMER);
    }

    private void handleRecord(MapRecord<String, String, Object> record) {
        try {
            boolean success = messageHandler.handleMessage(record);

            if (success) {
                streamOperations.acknowledge(notificationStreamKey, NotificationRedisConstants.CONSUMER_GROUP, record.getId());
                processedMessages.incrementAndGet();
//                log.debug("메시지 처리 성공 및 ACK: recordId={}", record.getId());
            } else {
                failedMessages.incrementAndGet();
//                log.warn("메시지 처리 실패 - Pending List에 유지: recordId={}", record.getId());
            }

        } catch (Exception e) {
            failedMessages.incrementAndGet();
            messageHandler.handleFailure(record, e);
//            log.error("메시지 처리 중 예외: recordId={}", record.getId(), e);
        }
    }

    @PreDestroy
    private void cleanup() {
        stop();
    }
}
