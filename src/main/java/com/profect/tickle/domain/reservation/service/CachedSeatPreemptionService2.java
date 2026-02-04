package com.profect.tickle.domain.reservation.service;

//import com.github.benmanes.caffeine.cache.Cache;
import com.profect.tickle.domain.member.entity.Member;
import com.profect.tickle.domain.member.repository.MemberRepository;
import com.profect.tickle.domain.reservation.config.SeatPreemptionConfig;
import com.profect.tickle.domain.reservation.dto.PreemptionContext;
import com.profect.tickle.domain.reservation.dto.request.SeatPreemptionRequestDto;
import com.profect.tickle.domain.reservation.dto.response.preemption.PreemptedSeatInfo;
import com.profect.tickle.domain.reservation.dto.response.preemption.SeatPreemptionResponseDto;
import com.profect.tickle.domain.reservation.entity.Seat;
import com.profect.tickle.domain.reservation.repository.SeatRepository;
import com.profect.tickle.global.exception.BusinessException;
import com.profect.tickle.global.exception.ErrorCode;
import com.profect.tickle.global.status.Status;
import com.profect.tickle.global.status.StatusIds;
import com.profect.tickle.global.status.service.StatusProvider;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class CachedSeatPreemptionService2 {

//    private static final String UNAVAILABLE_SEAT_MESSAGE = "선택한 좌석 중 선점할 수 없는 좌석이 있습니다.";
//    private static final String PREEMPTED_SEAT_KEY = "preempted:seat:";
//
//    private final SeatRepository seatRepository;
//    private final MemberRepository memberRepository;
//    private final StatusProvider statusProvider;
//    private final SeatPreemptionConfig config;
//    private final RedisTemplate<String, Object> redisTemplate;
//
//    // 선점된 좌석만 로컬 캐싱
//    @Qualifier("seatPreemptionCache")
//    private final Cache<String, Boolean> localPreemptionCache;
//
//    @Transactional
//    public SeatPreemptionResponseDto preemptSeats(SeatPreemptionRequestDto request, Long memberId) {
//
//        long startTime = System.currentTimeMillis();
//
//        try {
//            // 사용자 좌석 제한 검증 (캐싱 없이 기존 방식)
//            validateUserSeatLimits(request, memberId);
//
//            // 1. 로컬 캐시 + Redis 캐시로 이미 선점된 좌석 확인 (빠른 실패!)
//            List<Long> preemptedSeatIds = checkPreemptedSeatsInCache(request.getSeatIds());
//            if (!preemptedSeatIds.isEmpty()) {
//                long responseTime = System.currentTimeMillis() - startTime;
//                log.info("캐시에서 선점된 좌석 발견으로 빠른 응답 - {}ms, 좌석: {}", responseTime, preemptedSeatIds);
//                return SeatPreemptionResponseDto.failure(UNAVAILABLE_SEAT_MESSAGE, preemptedSeatIds);
//            }
//
//            // 2. DB에서 좌석 조회 및 선점 가능 여부 확인 (기존 방식)
//            List<Seat> seats = seatRepository.findAllByIdWithLock(request.getSeatIds());
//            List<Seat> availableSeats = filterAvailableSeats(seats, request.getPerformanceId());
//
//            // 3. 전체 선점 가능 여부 확인
//            if (hasUnavailableSeats(request, availableSeats)) {
//                List<Long> unavailableSeatIds = getUnavailableSeatIds(seats, availableSeats);
//                // 선점 불가능한 좌석들을 캐시에 저장 (다른 요청들의 빠른 실패를 위해)
//                saveUnavailableSeatsToCache(unavailableSeatIds);
//                return SeatPreemptionResponseDto.failure(UNAVAILABLE_SEAT_MESSAGE, unavailableSeatIds);
//            }
//
//            // 4. 전체 좌석 선점 및 캐시 업데이트
//            SeatPreemptionResponseDto response = executePreemption(memberId, availableSeats);
//
//            long totalTime = System.currentTimeMillis() - startTime;
//            log.info("좌석 선점 완료 - 총 {}ms, 좌석 수: {}", totalTime, availableSeats.size());
//
//            return response;
//
//        } catch (Exception e) {
//            long totalTime = System.currentTimeMillis() - startTime;
//            log.error("좌석 선점 실패 - {}ms, 사용자: {}, 오류: {}", totalTime, memberId, e.getMessage());
//            throw e;
//        }
//    }
//
//    /**
//     * 사용자 좌석 제한 검증 (기존 방식, 캐싱 없음)
//     */
//    private void validateUserSeatLimits(SeatPreemptionRequestDto request, Long memberId) {
//        long reservedSeatCount = seatRepository.countReservedSeatsByUserAndPerformance(memberId,
//                request.getPerformanceId());
//
//        if (reservedSeatCount >= config.getMaxSeatsPerMember()) {
//            throw new BusinessException(
//                    String.format("이미 %d개 좌석을 예매했습니다. 더 이상 선점할 수 없습니다.",
//                            config.getMaxSeatsPerMember()),
//                    ErrorCode.SEAT_LIMIT_EXCEEDED);
//        }
//
//        if (reservedSeatCount + request.getSeatIds().size() > config.getMaxSeatsPerMember()) {
//            throw new BusinessException(
//                    String.format("총 좌석 수 %d개를 초과하여 선점할 수 없습니다. 현재 예매된 좌석: %d",
//                            config.getMaxSeatsPerMember(), reservedSeatCount),
//                    ErrorCode.SEAT_SELECTION_EXCEEDED
//            );
//        }
//    }
//
//    /**
//     * 로컬 캐시 + Redis 캐시로 선점된 좌석들 확인
//     */
//    private List<Long> checkPreemptedSeatsInCache(List<Long> seatIds) {
//        return seatIds.stream()
//                .filter(this::isSeatPreemptedWithLocalCache)
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * 2단계 캐시로 좌석 선점 상태 확인 (핵심 로직!)
//     */
//    private boolean isSeatPreemptedWithLocalCache(Long seatId) {
//        String cacheKey = PREEMPTED_SEAT_KEY + seatId;
//
//        // L1: 로컬 캐시 확인 (가장 빠름 - 0.1ms)
//        Boolean localCached = localPreemptionCache.getIfPresent(cacheKey);
//        if (localCached != null) {
//            log.debug("로컬 캐시 히트 - 좌석: {}", seatId);
//            return localCached;
//        }
//
//        // L2: Redis 캐시 확인 (2-5ms)
//        boolean preempted = Boolean.TRUE.equals(redisTemplate.hasKey(cacheKey));
//
//        // 로컬 캐시에 저장 (다음 요청을 위해)
//        localPreemptionCache.put(cacheKey, preempted);
//
//        log.debug("Redis 캐시 확인 후 로컬 캐시 저장 - 좌석: {}, 선점여부: {}", seatId, preempted);
//        return preempted;
//    }
//
//    // ============================================
//    // 기존 메서드들 (변경 없음)
//    // ============================================
//
//    private List<Seat> filterAvailableSeats(List<Seat> seats, Long performanceId) {
//        return seats.stream()
//                .filter(seat -> seat.belongsToPerformance(performanceId))
//                .filter(Seat::isAvailableForPreemption)
//                .collect(Collectors.toList());
//    }
//
//    private boolean hasUnavailableSeats(SeatPreemptionRequestDto request, List<Seat> availableSeats) {
//        return availableSeats.size() != request.getSeatIds().size();
//    }
//
//    private List<Long> getUnavailableSeatIds(List<Seat> allSeats, List<Seat> availableSeats) {
//        Set<Long> availableSeatIds = availableSeats.stream()
//                .map(Seat::getId)
//                .collect(Collectors.toSet());
//
//        return allSeats.stream()
//                .map(Seat::getId)
//                .filter(id -> !availableSeatIds.contains(id))
//                .toList();
//    }
//
//    /**
//     * 선점 불가능한 좌석들을 양쪽 캐시에 저장 (빠른 실패를 위해)
//     */
//    private void saveUnavailableSeatsToCache(List<Long> seatIds) {
//        Duration cacheDuration = Duration.ofMinutes(config.getPreemptionDurationMinutes() + 1);
//
//        seatIds.forEach(seatId -> {
//            String cacheKey = PREEMPTED_SEAT_KEY + seatId;
//
//            // Redis 캐시 저장
//            redisTemplate.opsForValue().set(cacheKey, "PREEMPTED", cacheDuration);
//
//            // 로컬 캐시에도 저장 (다음 요청의 빠른 실패를 위해)
//            localPreemptionCache.put(cacheKey, true);
//        });
//
//        log.debug("선점 불가능한 좌석 {}개를 Redis + 로컬 캐시에 저장", seatIds.size());
//    }
//
//    /**
//     * 선점 성공한 좌석들을 양쪽 캐시에 저장
//     */
//    private void savePreemptedSeatsToCache(List<Seat> seats, Instant preemptedUntil) {
//        Duration cacheDuration = Duration.between(Instant.now(), preemptedUntil).plusMinutes(1);
//
//        seats.forEach(seat -> {
//            String cacheKey = PREEMPTED_SEAT_KEY + seat.getId();
//
//            // Redis 캐시 저장
//            redisTemplate.opsForValue().set(cacheKey, seat.getPreemptionToken(), cacheDuration);
//
//            // 로컬 캐시에도 저장
//            localPreemptionCache.put(cacheKey, true);
//        });
//
//        log.debug("선점된 좌석 {}개를 Redis + 로컬 캐시에 저장", seats.size());
//    }
//
//    private SeatPreemptionResponseDto executePreemption(Long memberId, List<Seat> seats) {
//        try {
//            PreemptionContext context = createPreemptionContext(memberId);
//            List<Seat> preemptedSeats = performSeatPreemption(seats, context);
//
//            // 선점된 좌석들을 양쪽 캐시에 저장
//            savePreemptedSeatsToCache(preemptedSeats, context.getPreemptedUntil());
//
//            List<PreemptedSeatInfo> preemptedSeatInfos = convertToPreemptedSeatInfos(preemptedSeats);
//
//            log.info("좌석 선점 완료 - 사용자: {}, 선점된 좌석 수: {}, 토큰: {}, 만료 시간: {}",
//                    memberId, preemptedSeats.size(), context.getPreemptionToken(),
//                    context.getPreemptedUntil());
//
//            return SeatPreemptionResponseDto.success(
//                    context.getPreemptionToken(),
//                    context.getPreemptedUntil(),
//                    preemptedSeatInfos,
//                    String.format("%d개 좌석을 선점했습니다.", preemptedSeats.size()));
//
//        } catch (Exception e) {
//            log.error("좌석 선점 중 오류 발생 - 사용자: {}, 좌석 IDs: {}", memberId,
//                    seats.stream().map(Seat::getId).collect(Collectors.toList()), e);
//            throw new BusinessException(e.getMessage(), ErrorCode.SEAT_PREEMPTION_EXCEPTION);
//        }
//    }
//
//    private PreemptionContext createPreemptionContext(Long userId) {
//        String preemptionToken = generatePreemptionToken();
//        Instant preemptedAt = Instant.now();
//        Instant preemptedUntil = Instant.now()
//                .plus(config.getPreemptionDurationMinutes(), ChronoUnit.MINUTES);
//        Member member = memberRepository.findById(userId)
//                .orElseThrow(() -> new BusinessException(ErrorCode.MEMBER_NOT_FOUND));
//
//        return PreemptionContext.builder()
//                .preemptionToken(preemptionToken)
//                .preemptedAt(preemptedAt)
//                .preemptedUntil(preemptedUntil)
//                .member(member)
//                .build();
//    }
//
//    private List<Seat> performSeatPreemption(List<Seat> seats, PreemptionContext context) {
//        Status preemptedStatus = statusProvider.provide(StatusIds.Seat.PREEMPTED);
//
//        seats.forEach(seat -> seat.preempt(
//                context.getPreemptionToken(),
//                context.getPreemptedAt(),
//                context.getPreemptedUntil(),
//                context.getMember(),
//                preemptedStatus
//        ));
//
//        return seatRepository.saveAll(seats);
//    }
//
//    private String generatePreemptionToken() {
//        return UUID.randomUUID().toString();
//    }
//
//    private List<PreemptedSeatInfo> convertToPreemptedSeatInfos(List<Seat> preemptedSeats) {
//        return preemptedSeats.stream()
//                .map(this::convertToPreemptedSeatInfo)
//                .collect(Collectors.toList());
//    }
//
//    private PreemptedSeatInfo convertToPreemptedSeatInfo(Seat seat) {
//        return PreemptedSeatInfo.builder()
//                .seatId(seat.getId())
//                .seatNumber(seat.getSeatNumber())
//                .seatGrade(seat.getSeatGrade())
//                .seatPrice(seat.getSeatPrice())
//                .build();
//    }
//
//    /**
//     * 캐시 통계 조회 (모니터링용)
//     */
//    public String getCacheStats() {
//        return String.format(
//                "선점 캐시 통계 - 히트율 %.2f%%, 요청수 %d, 캐시 크기: %d개",
//                localPreemptionCache.stats().hitRate() * 100,
//                localPreemptionCache.stats().requestCount(),
//                localPreemptionCache.estimatedSize()
//        );
//    }
//
//    /**
//     * 특정 좌석의 캐시 상태 확인 (디버깅용)
//     */
//    public boolean checkSeatCacheStatus(Long seatId) {
//        String cacheKey = PREEMPTED_SEAT_KEY + seatId;
//        Boolean localResult = localPreemptionCache.getIfPresent(cacheKey);
//        boolean redisResult = Boolean.TRUE.equals(redisTemplate.hasKey(cacheKey));
//
//        log.info("좌석 {} 캐시 상태 - 로컬: {}, Redis: {}", seatId, localResult, redisResult);
//        return localResult != null ? localResult : redisResult;
//    }
}