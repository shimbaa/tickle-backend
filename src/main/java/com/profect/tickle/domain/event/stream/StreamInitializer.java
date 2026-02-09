package com.profect.tickle.domain.event.stream;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RKeys;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Profile("redis")
public class StreamInitializer {

    public static final String STREAM_KEY = "stream:ticket-events";
    public static final String GROUP      = "ticket-workers";

    private final RedissonClient redisson;
    private final TypedJsonJacksonCodec streamFieldMapCodec;

    @PostConstruct
    public void init() {
        RStream<String, String> stream = redisson.getStream(STREAM_KEY, StringCodec.INSTANCE);
        RKeys keys = redisson.getKeys();

        // 1) 스트림 키 보장
        StreamMessageId bootstrapId = null;
        if (keys.countExists(STREAM_KEY) == 0) {
            bootstrapId = stream.add(StreamAddArgs.entries(Map.of("_bootstrap", "1")));
        }

        // 2) 그룹 존재 확인 (이제 안전하게 호출 가능)
        boolean hasGroup = false;
        try {
            hasGroup = stream.listGroups().stream()
                    .anyMatch(g -> GROUP.equals(g.getName()));
        } catch (Exception ignore) {
            // 키가 막 생겼거나 일시 오류면 없다고 보고 아래서 생성
        }

        // 3) 그룹 생성 (경쟁적으로 동시에 생성될 수 있으므로 BUSYGROUP는 무시)
        if (!hasGroup) {
            try {
                stream.createGroup(GROUP, StreamMessageId.NEWEST); // '$' 의미
            } catch (Exception e) {
                // 이미 다른 인스턴스가 만들었다면 "BUSYGROUP" 류 예외가 올 수 있음 → 무시
                String msg = String.valueOf(e.getMessage());
                if (!msg.contains("BUSYGROUP")) {
                    throw e;
                }
            }
        }

        // 4) 부트스트랩 메시지 제거
        if (bootstrapId != null) {
            try {
                stream.remove(bootstrapId);
            } catch (Exception ignore) { /* 없어도 무시 */ }
        }
    }
}