package com.taobao.search.iquan.core.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.common.SqlExecPhase;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class GuavaCacheUtils {
    private static final Logger logger = LoggerFactory.getLogger(GuavaCacheUtils.class);

    public static <K, V> Cache<K, V> newGuavaCache() {
        Cache<K, V> cache = CacheBuilder.newBuilder()
                .initialCapacity(1024)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() - 1)
                .maximumSize(10240)
                .expireAfterWrite(300, TimeUnit.SECONDS)
                //.expireAfterAccess(300, TimeUnit.SECONDS)
                .recordStats()
                .build();

        return cache;
    }

    public static <K, V> Cache<K, V> newGuavaCache(int initialCapacity, int concurrencyLevel, long maximumSize, long expireAfterWrite) {
        Cache<K, V> cache = CacheBuilder.newBuilder()
                .initialCapacity(initialCapacity)
                .concurrencyLevel(concurrencyLevel)
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
                //.expireAfterAccess(300, TimeUnit.SECONDS)
                .recordStats()
                .build();

        return cache;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Cache<K, V> getGuavaCache(String cacheName, Map<String, Cache<?, ?>> cacheMap) {
        if (!cacheMap.containsKey(cacheName)) {
            return null;
        }

        Cache<?, ?> cache = cacheMap.get(cacheName);
        try {
            return (Cache<K, V>) cache;
        } catch (Exception ex) {
            logger.error("cast cache fail: ", ex);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> V getGuavaCacheValue(String cacheName, K key, Map<String, Cache<?, ?>> cacheMap) {
        if (!cacheMap.containsKey(cacheName)) {
            return null;
        }

        Cache<?, ?> cache = cacheMap.get(cacheName);
        try {
            Cache<K, V> cache1 = (Cache<K, V>) cache;

            int rand = ThreadLocalRandom.current().nextInt(0, 10000);
            if (rand < 5) {
                logger.info("cache name {}, size {}, stats [{}]", cacheName, cache1.size(), cache1.stats().toString());
            }

            return cache1.getIfPresent(key);
        } catch (Exception ex) {
            logger.error("cast cache fail: ", ex);
            return null;
        }
    }

    public static boolean initSqlExecPhaseCache(SqlTranslator sqlTranslator, int initialCapacity, int concurrencyLevel, long maximumSize, long expireAfterWrite) {
        Set<SqlExecPhase> validPhases = SqlExecPhase.PlanPrepareLevel.getValidPhases();
        for (SqlExecPhase phase : validPhases) {
            if (phase == SqlExecPhase.START || phase == SqlExecPhase.END) {
                continue;
            }

            if (phase.getName().startsWith("sql")) {
                sqlTranslator.<String, List<SqlNode>>addCache(
                        phase.getName(), initialCapacity, concurrencyLevel, maximumSize, expireAfterWrite);
            } else if (phase.getName().startsWith("rel")) {
                sqlTranslator.<String, List<RelNode>>addCache(
                        phase.getName(), initialCapacity, concurrencyLevel, maximumSize, expireAfterWrite);
            } else if (phase.getName().startsWith("jni")) {
                // skip it
            } else {
                throw new RuntimeException("Impossible path");
            }

            logger.info("init cache {} success, ialCapacity {}, concurrencyLevel {}, maximumSize {}, expireAfterWrite {}",
                    phase.getName(), initialCapacity, concurrencyLevel, maximumSize, expireAfterWrite);
        }
        return true;
    }
}
