package com.github.sibdevtools.storage.embedded.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.sibdevtools.session.api.rq.SetValueRq;
import com.github.sibdevtools.session.api.rs.GetValueRs;
import com.github.sibdevtools.session.api.rs.SetValueRs;
import com.github.sibdevtools.session.api.service.KeyValueStorageService;
import com.github.sibdevtools.storage.embedded.dto.ValueHolderImpl;
import com.github.sibdevtools.storage.embedded.dto.ValueMetaImpl;
import com.github.sibdevtools.storage.embedded.exception.UnexpectedErrorException;
import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.val;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Implementation of in-memory key-value storage
 *
 * @author sibmaks
 * @since 0.0.1
 */
public class KeyValueStorageServiceEmbedded implements KeyValueStorageService {
    private final Map<String, Cache<String, CachedValue>> spaces = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    public Set<String> getSpaces() {
        return new HashSet<>(spaces.keySet());
    }

    @Nonnull
    @Override
    public Set<String> getKeys(@Nonnull String space) {
        val cache = spaces.get(space);
        if (cache == null) {
            return Collections.emptySet();
        }

        val all = cache.asMap();
        return all.entrySet().stream()
                .filter(e -> !e.getValue().isExpired())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public void delete(@Nonnull String space) {
        spaces.remove(space);
    }

    @Nonnull
    @Override
    public Optional<GetValueRs> get(
            @Nonnull String space,
            @Nonnull String key
    ) {
        val cache = spaces.get(space);
        if (cache == null) {
            return Optional.empty();
        }

        val value = cache.getIfPresent(key);
        if (value == null || value.isExpired()) {
            cache.invalidate(key);
            return Optional.empty();
        }

        val valueMeta = value.buildValueMeta();

        val valueHolder = ValueHolderImpl.builder()
                .value(value.value)
                .meta(valueMeta)
                .build();

        return Optional.of(new GetValueRs(valueHolder));
    }

    @Nonnull
    @Override
    public SetValueRs set(@Nonnull SetValueRq rq) {
        val space = rq.getSpace();
        val key = rq.getKey();
        val value = rq.getValue();
        val expiredAt = rq.getExpiredAt();

        val cache = spaces.computeIfAbsent(space, s -> Caffeine.newBuilder()
                .expireAfter(new Expiry<String, CachedValue>() {
                    @Override
                    public long expireAfterCreate(String key, CachedValue value, long currentTime) {
                        return toNanos(value.expiredAt);
                    }

                    @Override
                    public long expireAfterUpdate(String key, CachedValue value, long currentTime, long currentDuration) {
                        return toNanos(value.expiredAt);
                    }

                    @Override
                    public long expireAfterRead(String key, CachedValue value, long currentTime, long currentDuration) {
                        return currentDuration; // don't change on access
                    }

                    private long toNanos(ZonedDateTime time) {
                        if (time == null) return Long.MAX_VALUE;
                        val duration = Duration.between(ZonedDateTime.now(), time);
                        return Math.max(0, duration.toNanos());
                    }
                })
                .build());

        val result = cache.asMap().compute(key, (k, old) -> {
            if (old == null || old.isExpired()) {
                return new CachedValue(value, expiredAt);
            }
            if (!Arrays.equals(old.value, value)) {
                return old.modify(value, expiredAt);
            }
            return old;
        });

        return new SetValueRs(result.buildValueMeta());
    }

    @Nonnull
    @Override
    public SetValueRs prolongate(
            @Nonnull String space,
            @Nonnull String key,
            @Nonnull ZonedDateTime expiredAt
    ) {
        val cache = spaces.get(space);
        if (cache == null) {
            throw new UnexpectedErrorException("Space not found");
        }

        val old = cache.getIfPresent(key);
        if (old == null || old.isExpired()) {
            cache.invalidate(key);
            throw new UnexpectedErrorException("Key not found or expired");
        }

        val updated = old.prolongate(expiredAt);
        cache.put(key, updated);

        val valueMeta = updated.buildValueMeta();

        return new SetValueRs(valueMeta);
    }

    @Override
    public void delete(@Nonnull String space, @Nonnull String key) {
        val cache = spaces.get(space);
        if (cache != null) {
            cache.invalidate(key);
        }
    }

    @Builder
    @AllArgsConstructor
    private static class CachedValue {
        final byte[] value;
        final long version;
        final ZonedDateTime createdAt;
        final ZonedDateTime modifiedAt;
        final ZonedDateTime expiredAt;

        CachedValue(byte[] value, ZonedDateTime expiredAt) {
            this.value = value;
            this.version = 0;
            this.createdAt = ZonedDateTime.now();
            this.modifiedAt = ZonedDateTime.now();
            this.expiredAt = expiredAt;
        }

        boolean isExpired() {
            return expiredAt != null && ZonedDateTime.now().isAfter(expiredAt);
        }

        public CachedValue modify(byte[] value, ZonedDateTime expiredAt) {
            return CachedValue.builder()
                    .value(value)
                    .version(this.version + 1)
                    .createdAt(this.createdAt)
                    .modifiedAt(ZonedDateTime.now())
                    .expiredAt(expiredAt)
                    .build();
        }

        public CachedValue prolongate(ZonedDateTime expiredAt) {
            return CachedValue.builder()
                    .value(value)
                    .version(this.version)
                    .createdAt(this.createdAt)
                    .modifiedAt(this.modifiedAt)
                    .expiredAt(expiredAt)
                    .build();
        }

        public ValueMetaImpl buildValueMeta() {
            return ValueMetaImpl.builder()
                    .version(version)
                    .createdAt(createdAt)
                    .modifiedAt(modifiedAt)
                    .expiredAt(expiredAt)
                    .build();
        }
    }
}
