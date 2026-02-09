package com.profect.tickle.domain.notification.service.realtime.producer;

import com.profect.tickle.domain.notification.dto.NotificationEnvelope;
import com.profect.tickle.global.exception.BusinessException;
import com.profect.tickle.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
@Profile("redis")
public class RedisNotificationProducer implements MessageProducer {

    private final StreamOperations<String, String, Object> streamOperations;

    @Value("#{@notificationStreamKey}")
    private String notificationStreamKey;

    @Override
    public void produce(String key, NotificationEnvelope<?> envelope) {
        try {
            Map<String, Object> messageData = Map.of(
                    "type", envelope.type().name(),
                    "receivedMemberId", envelope.receivedMemberId() != null ?
                            envelope.receivedMemberId().toString() : "",
                    "subject", envelope.subject(),
                    "content", envelope.content(),
                    "createdAt", envelope.createdAt().toString(),
                    "link", envelope.link() != null ? envelope.link() : "",
                    "data", envelope.data() != null ? envelope.data().toString() : ""
            );

            streamOperations.add(notificationStreamKey, messageData);

            log.debug("Redis Stream 메시지 발행 완료: key={}, type={}, memberId={}",
                    key, envelope.type(), envelope.receivedMemberId());

        } catch (Exception e) {
            log.error("Redis Stream 메시지 발행 실패: key={}, type={}, memberId={}, error={}",
                    key, envelope.type(), envelope.receivedMemberId(), e.getMessage(), e);
            throw new BusinessException("Redis Stream 메시지 발행 실패", ErrorCode.MESSAGE_PRODUCE_FAILED);
        }
    }
}
