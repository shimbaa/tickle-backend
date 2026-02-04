package com.profect.tickle.global.redis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.retry.annotation.EnableRetry;

import java.time.Duration;

@Configuration
@EnableCaching
@EnableRetry
public class RedisConfig {

    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Value("${spring.data.redis.password}")
    private String password;

    private static final String REDISSON_HOST_PREFIX = "redis://";

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();

        config.setCodec(new org.redisson.client.codec.StringCodec());

        SingleServerConfig s = config.useSingleServer()
                .setAddress(REDISSON_HOST_PREFIX + host + ":" + port)
//                .setPassword(password)
                .setConnectionMinimumIdleSize(8)
                .setConnectionPoolSize(32)
                .setSubscriptionConnectionMinimumIdleSize(2)
                .setSubscriptionConnectionPoolSize(8)
                .setIdleConnectionTimeout(10_000)
                .setConnectTimeout(10_000)
                .setTimeout(3_000)
                .setRetryAttempts(3)
                .setRetryInterval(1_000)
                .setKeepAlive(true);

        config.setThreads(12);
        config.setNettyThreads(12);

        return Redisson.create(config);
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration();
        redisConfig.setHostName(host);
        redisConfig.setPort(port);
        redisConfig.setPassword(password);

        // Lettuce Pool 설정 (선택사항)
        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .poolConfig(new GenericObjectPoolConfig<>())
                .build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                redisConfig, clientConfig);

        factory.afterPropertiesSet();

        return factory;
    }

    /**
     * Redis 데이터 처리를 위한 템플릿을 구성합니다.
     * 해당 구성된 RedisTemplate을 통해서 데이터 통신으로 처리되는 대한 직렬화를 수행합니다.
     *
     * @return RedisTemplate<String, Object>
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();

        // Redis를 연결합니다.
        redisTemplate.setConnectionFactory(redisConnectionFactory());

        // Key-Value 형태로 직렬화를 수행합니다.
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());

        // Hash Key-Value 형태로 직렬화를 수행합니다.
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());

        // 기본적으로 직렬화를 수행합니다.
        redisTemplate.setDefaultSerializer(new StringRedisSerializer());

        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    /**
     * 리스트에 접근하여 다양한 연산을 수행합니다.
     *
     * @return ListOperations<String, Object>
     */
    public ListOperations<String, Object> getListOperations() {
        return this.redisTemplate().opsForList();
    }

    /**
     * 단일 데이터에 접근하여 다양한 연산을 수행합니다.
     *
     * @return ValueOperations<String, Object>
     */
    public ValueOperations<String, Object> getValueOperations() {
        return this.redisTemplate().opsForValue();
    }


    /**
     * Redis 작업중 등록, 수정, 삭제에 대해서 처리 및 예외처리를 수행합니다.
     *
     * @param operation
     * @return
     */
    public int executeOperation(Runnable operation) {
        try {
            operation.run();
            return 1;
        } catch (Exception e) {
            System.out.println("Redis 작업 오류 발생 :: " + e.getMessage());
            return 0;
        }
    }

    @Bean
    public RTopic ticketTopic(RedissonClient redissonClient) {
        return redissonClient.getTopic("ticketEvent", new JsonJacksonCodec());
    }

    /**
     * CacheManager 설정
     */
    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory) {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(10));  // 10분 TTL

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(config)
                .build();
    }

    @Bean("streamRedisTemplate")
    public RedisTemplate<String, Object> streamRedisTemplate() {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory());

        // Stream용으로는 JSON 직렬화 사용
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());

        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public StreamOperations<String, String, Object> streamOperations(
            @Qualifier("streamRedisTemplate") RedisTemplate<String, Object> streamRedisTemplate) {
        return streamRedisTemplate.opsForStream();
    }

    @Bean("notificationStreamKey")
    public String notificationStreamKey() {
        return "notification-stream";
    }
}
