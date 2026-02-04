//package com.profect.tickle.global.config;
//
//import com.github.benmanes.caffeine.cache.Cache;
//import com.github.benmanes.caffeine.cache.Caffeine;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//import java.time.Duration;
//
///**
// * 선점된 좌석만 캐싱하는 단순한 Caffeine 설정
// */
//@Configuration
//public class CaffeineCacheConfig {
//
//    /**
//     * 선점된 좌석 캐시
//     * - 목적: 이미 선점된 좌석에 대한 빠른 실패 응답
//     * - TTL: 30초 (선점 시간보다 짧게)
//     * - 크기: 10,000개 (메모리 ~300KB)
//     */
//    @Bean("seatPreemptionCache")
//    public Cache<String, Boolean> seatPreemptionCache() {
//        return Caffeine.newBuilder()
//                .maximumSize(10_000)
//                .expireAfterWrite(Duration.ofSeconds(30))
//                .recordStats() // 캐시 성능 측정
//                .build();
//    }
//}