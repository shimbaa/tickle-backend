package com.profect.tickle.domain.notification.service.realtime.recovery;

import com.profect.tickle.domain.notification.dto.NotificationEnvelope;
import com.profect.tickle.domain.notification.service.realtime.MessageQueueRecoveryService;
import com.profect.tickle.domain.notification.service.realtime.RealtimeSender;
import com.profect.tickle.domain.notification.util.NotificationEnvelopeConverter;
import com.profect.tickle.domain.notification.util.constant.NotificationRedisConstants;
import com.profect.tickle.domain.notification.util.constant.NotificationSchedulerConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
@Profile("!local")
public class RedisMessageQueueRecoveryService implements MessageQueueRecoveryService {

    private final StreamOperations<String, String, Object> streamOperations;
    private final RealtimeSender realtimeSender;
    private final NotificationEnvelopeConverter envelopeConverter;
    private final MeterRegistry meterRegistry;

    @Value("#{@notificationStreamKey}")
    private String notificationStreamKey;

    private Counter messagesRecovered;
    private Counter messagesDiscarded;

    @PostConstruct
    private void initMetrics() {
        this.messagesRecovered = Counter.builder("notification_recovery_messages_recovered_total")
                .description("복구된 Pending 메시지 총 개수")
                .register(meterRegistry);

        this.messagesDiscarded = Counter.builder("notification_recovery_messages_discarded_total")
                .description("복구 중 폐기된 메시지 총 개수")
                .register(meterRegistry);
    }

    @Override
    public void recoverPendingMessages() {
        try {
            PendingMessagesSummary summary = streamOperations.pending(notificationStreamKey, NotificationRedisConstants.CONSUMER_GROUP);

            if (summary.getTotalPendingMessages() == 0) {
                log.debug("처리할 Pending 메시지 없음");
                return;
            }

            log.info("Pending 메시지 복구 시작: {} 건", summary.getTotalPendingMessages());

            Map<String, Long> consumersWithPendingMessages = summary.getPendingMessagesPerConsumer();

            int recoveredCount = 0;
            int skippedCount = 0;
            int discardedCount = 0;
            int processedCount = 0;

            for (String consumerName : consumersWithPendingMessages.keySet()) {
                if (processedCount >= NotificationSchedulerConstants.MAX_RECOVERY_BATCH_SIZE) {
                    break;
                }

                PendingMessages pendingMessages = streamOperations.pending(
                        notificationStreamKey,
                        Consumer.from(NotificationRedisConstants.CONSUMER_GROUP, consumerName)
                );

                for (PendingMessage pendingMessage : pendingMessages) {
                    if (processedCount >= NotificationSchedulerConstants.MAX_RECOVERY_BATCH_SIZE) break;

                    long elapsedMinutes = pendingMessage.getElapsedTimeSinceLastDelivery().toMinutes();

                    if (elapsedMinutes >= NotificationSchedulerConstants.RECOVERY_THRESHOLD_MINUTES) {
                        if (elapsedMinutes >= NotificationSchedulerConstants.DISCARD_THRESHOLD_MINUTES) {
                            boolean discarded = discardOldMessage(pendingMessage);
                            if (discarded) {
                                discardedCount++;
                                messagesDiscarded.increment();
                            }
                            processedCount++;
                            continue;
                        }

                        boolean recovered = recoverSingleMessage(pendingMessage);
                        if (recovered) {
                            recoveredCount++;
                            messagesRecovered.increment();
                        } else {
                            skippedCount++;
                        }
                        processedCount++;
                    } else {
                        skippedCount++;
                        processedCount++;
                    }
                }
            }

            log.info("Pending 메시지 복구 완료: 복구={} 건, 스킵={} 건, 폐기={} 건", recoveredCount, skippedCount, discardedCount);

        } catch (Exception e) {
            log.error("Pending 메시지 복구 중 오류", e);
        }
    }

    @Override
    public boolean recoverMessage(String messageId) {
        try {
            List<MapRecord<String, String, Object>> claimedMessages =
                    streamOperations.claim(
                            notificationStreamKey,
                            NotificationRedisConstants.CONSUMER_GROUP,
                            NotificationRedisConstants.RECOVERY_CONSUMER,
                            Duration.ofMinutes(10),
                            RecordId.of(messageId)
                    );

            if (claimedMessages.isEmpty()) {
                log.debug("Claim할 메시지 없음: {}", messageId);
                return false;
            }

            for (MapRecord<String, String, Object> record : claimedMessages) {
                boolean success = reprocessMessage(record);
                if (success) {
                    streamOperations.acknowledge(notificationStreamKey,
                            NotificationRedisConstants.CONSUMER_GROUP,
                            record.getId());
                    log.info("메시지 복구 성공: recordId={}", record.getId());
                    return true;
                } else {
                    log.warn("메시지 복구 실패: recordId={} - 다음 복구 주기에서 재시도", record.getId());
                    return false;
                }
            }
        } catch (Exception e) {
            log.error("메시지 복구 중 예외: messageId={}", messageId, e);
            return false;
        }
        return false;
    }

    @Override
    public boolean discardMessage(String messageId, Duration threshold) {
        try {
            List<MapRecord<String, String, Object>> claimedMessages =
                    streamOperations.claim(
                            notificationStreamKey,
                            NotificationRedisConstants.CONSUMER_GROUP,
                            NotificationRedisConstants.RECOVERY_CONSUMER,
                            Duration.ofMinutes(1),
                            RecordId.of(messageId)
                    );

            for (MapRecord<String, String, Object> record : claimedMessages) {
                streamOperations.acknowledge(notificationStreamKey,
                        NotificationRedisConstants.CONSUMER_GROUP,
                        record.getId());
                log.warn("오래된 메시지 폐기: recordId={}, threshold={}분",
                        record.getId(), threshold.toMinutes());
            }

            return !claimedMessages.isEmpty();

        } catch (Exception e) {
            log.error("오래된 메시지 폐기 중 예외: messageId={}", messageId, e);
            return false;
        }
    }

    private boolean recoverSingleMessage(PendingMessage pendingMessage) {
        try {
            List<MapRecord<String, String, Object>> claimedMessages =
                    streamOperations.claim(
                            notificationStreamKey,
                            NotificationRedisConstants.CONSUMER_GROUP,
                            NotificationRedisConstants.RECOVERY_CONSUMER,
                            Duration.ofMinutes(10),
                            pendingMessage.getId()
                    );

            if (claimedMessages.isEmpty()) {
                log.debug("Claim할 메시지 없음: {}", pendingMessage.getIdAsString());
                return false;
            }

            for (MapRecord<String, String, Object> record : claimedMessages) {
                boolean success = reprocessMessage(record);

                if (success) {
                    streamOperations.acknowledge(notificationStreamKey, NotificationRedisConstants.CONSUMER_GROUP, record.getId());
                    log.info("메시지 복구 성공: recordId={}", record.getId());
                    return true;
                } else {
                    log.warn("메시지 복구 실패: recordId={} - 다음 복구 주기에서 재시도", record.getId());
                    return false;
                }
            }

        } catch (Exception e) {
            log.error("메시지 복구 중 예외: messageId={}", pendingMessage.getIdAsString(), e);
        }

        return false;
    }

    private boolean reprocessMessage(MapRecord<String, String, Object> record) {
        try {
            Map<String, Object> recordValue = record.getValue();
            log.debug("메시지 재처리 시작: recordId={}, type={}",
                    record.getId(), recordValue.get("type"));

            NotificationEnvelope<Object> envelope = envelopeConverter.convertToNotificationEnvelope(recordValue);

            switch (envelope.type()) {
                case PARTNER_PERFORMANCE_PUBLISHED -> {
                    boolean success = realtimeSender.sendAll(envelope);
                    if (success) {
                        log.debug("브로드캐스트 재처리 성공: recordId={}", record.getId());
                        return true;
                    } else {
                        log.debug("브로드캐스트 재처리 실패: recordId={}", record.getId());
                        return false;
                    }
                }

                case RESERVATION_SUCCESS, PERFORMANCE_MODIFIED, COUPON_ALMOST_EXPIRED, AUTH_CODE_SENT -> {
                    Long memberId = envelope.receivedMemberId();
                    boolean success = realtimeSender.send(memberId, envelope);
                    if (success) {
                        log.debug("개별 사용자 재처리 성공: recordId={}, memberId={}",
                                record.getId(), memberId);
                        return true;
                    } else {
                        log.debug("개별 사용자 재처리 실패: recordId={}, memberId={}",
                                record.getId(), memberId);
                        return false;
                    }
                }

                default -> {
                    log.warn("재처리할 수 없는 메시지 타입: recordId={}, type={}",
                            record.getId(), envelope.type());
                    return false;
                }
            }

        } catch (Exception e) {
            log.error("메시지 재처리 중 예외: recordId={}", record.getId(), e);
            return false;
        }
    }

    private boolean discardOldMessage(PendingMessage pendingMessage) {
        try {
            List<MapRecord<String, String, Object>> claimedMessages =
                    streamOperations.claim(
                            notificationStreamKey,
                            NotificationRedisConstants.CONSUMER_GROUP,
                            NotificationRedisConstants.RECOVERY_CONSUMER,
                            Duration.ofMinutes(1),
                            pendingMessage.getId()
                    );

            for (MapRecord<String, String, Object> record : claimedMessages) {
                streamOperations.acknowledge(notificationStreamKey, NotificationRedisConstants.CONSUMER_GROUP, record.getId());
                log.warn("오래된 메시지 폐기: recordId={}, 경과시간={}분",
                        record.getId(), pendingMessage.getElapsedTimeSinceLastDelivery().toMinutes());
            }

            return !claimedMessages.isEmpty();

        } catch (Exception e) {
            log.error("오래된 메시지 폐기 중 예외: messageId={}", pendingMessage.getIdAsString(), e);
            return false;
        }
    }
}
