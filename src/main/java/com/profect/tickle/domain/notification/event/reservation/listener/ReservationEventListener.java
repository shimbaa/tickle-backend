package com.profect.tickle.domain.notification.event.reservation.listener;

import com.profect.tickle.domain.notification.dto.NotificationEnvelope;
import com.profect.tickle.domain.notification.dto.request.MailCreateServiceRequestDto;
import com.profect.tickle.domain.notification.entity.NotificationKind;
import com.profect.tickle.domain.notification.entity.NotificationTemplate;
import com.profect.tickle.domain.notification.event.reservation.event.ReservationSuccessEvent;
import com.profect.tickle.domain.notification.service.NotificationService;
import com.profect.tickle.domain.notification.service.NotificationTemplateService;
import com.profect.tickle.domain.notification.service.mail.MailSender;
import com.profect.tickle.domain.notification.service.realtime.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Profile("redis")
@Slf4j
public class ReservationEventListener {

    // utils
    private final Clock clock;

    // services
    @Value("#{@notificationStreamKey}")
    private String notificationStreamKey;
    private final MessageProducer redisNotificationProducer;
    private final NotificationService notificationService;
    private final NotificationTemplateService notificationTemplateService;
    private final MailSender mailSender;

    // 예매 성공 시 알림 전송
    @EventListener
    public void handleReservationSuccess(ReservationSuccessEvent event) {
        log.info("[이벤트 감지] {}님이 공연 \"{}\" 예매 (code={})",
                event.reservation().getMemberEmail(),
                event.performance().title(),
                event.reservation().getCode()
        );

        NotificationTemplate template = notificationTemplateService
                .getNotificationTemplateById(NotificationKind.RESERVATION_SUCCESS.getId());

        String subject = String.format(template.getTitle(), event.performance().title());
        String content = String.format(template.getContent(),
                event.performance().title(),
                event.performance().performanceDateAndTime(),
                event.reservation().getCode()
        );
        Instant now = clock.instant();

        // 알림 저장
        notificationService.saveNotification(event.reservation().getMemberEmail(), template, subject, content, now);

        // 메일 전송
        try {
            mailSender.sendText(new MailCreateServiceRequestDto(event.reservation().getMemberEmail(), subject, content));
            log.info("메일 전송 성공");
        } catch (Exception e) {
            log.warn("메일 전송 실패 - 다른 알림 채널은 정상 처리: {}", e.getMessage());
        }

        // 실시간 알림 전송
        NotificationEnvelope<Void> payload = new NotificationEnvelope<>(
                NotificationKind.RESERVATION_SUCCESS,
                event.reservation().getMemberId(),
                subject,
                content,
                now,
                "https://tickle.kr/mypage/reservations",
                null
        );
        redisNotificationProducer.produce(notificationStreamKey, payload);
    }
}
