package com.taobao.search.iquan.boot.config;

import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.utils.GuavaCacheUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class SqlEnvConfig {
    private static final Logger logger = LoggerFactory.getLogger(SqlEnvConfig.class);

    @Value("${iquan.cache.initial.capacity}")
    private int initialCapacity = 0;
    @Value("${iquan.cache.concurrency.level}")
    private int concurrencyLevel = 0;
    @Value("${iquan.cache.maximum.size}")
    private long maximumSize = 0L;
    @Value("${iquan.cache.expire.after.write}")
    private long expireAfterWrite = 0L;

    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    @Bean
    public SqlTranslator sqlTranslator() {
        SqlTranslator sqlTranslator = new SqlTranslator(false);
        if (initialCapacity > 0
                && GuavaCacheUtils.initSqlExecPhaseCache(sqlTranslator,
                initialCapacity, concurrencyLevel, maximumSize, expireAfterWrite)) {
            logger.info("init all cache success.");
        }
        return sqlTranslator;
    }
}
