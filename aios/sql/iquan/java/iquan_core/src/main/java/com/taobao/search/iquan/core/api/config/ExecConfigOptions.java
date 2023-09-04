package com.taobao.search.iquan.core.api.config;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ExecConfigOptions {

    // ===========================================
    // Options
    // ===========================================
    public static final IquanConfiguration<String> SQL_OPTIMIZER_SQL_PARSE_TIME_US =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.sql.parse.time.us")
                    .clazz(String.class)
                    .defaultValue("0.0")
                    .description("sql parse time.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_CATALOG_REGISTER_TIME_US =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.catalog.register.time.us")
                    .clazz(String.class)
                    .defaultValue("0.0")
                    .description("catalog register time.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_SQL_VALIDATE_TIME_US =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.sql.validate.time.us")
                    .clazz(String.class)
                    .defaultValue("0.0")
                    .description("sql validate time.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_REL_TRANSFORM_TIME_US =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.rel.transform.time.us")
                    .clazz(String.class)
                    .defaultValue("0.0")
                    .description("sql to rel time.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_REL_OPTIMIZE_TIME_US =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.rel.optimize.time.us")
                    .clazz(String.class)
                    .defaultValue("0.0")
                    .description("rel optimize time.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_REL_POST_OPTIMIZE_TIME_US =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.rel.post.optimize.time.us")
                    .clazz(String.class)
                    .defaultValue("0.0")
                    .description("rel post optimize time.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_PLAN_PUT_CACHE =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.plan.put.cache")
                    .clazz(String.class)
                    .defaultValue("false")
                    .description("put plan cache.")
                    .build();

    public static final IquanConfiguration<String> SQL_OPTIMIZER_PLAN_GET_CACHE =
            IquanConfiguration.<String>builder()
                    .key("sql.optimizer.plan.get.cache")
                    .clazz(String.class)
                    .defaultValue("false")
                    .description("get plan cache.")
                    .build();

    public static final IquanConfiguration<String> EXEC_OPTIMIZER_STREAMING_BATCH_SIZE =
            IquanConfiguration.<String>builder()
                    .key("exec.optimizer.streaming.batch.size")
                    .clazz(String.class)
                    .defaultValue("8000")
                    .description("streaming batch size for exec pipeline.")
                    .build();


    // ===========================================
    // Note: should contain all option keys
    // ===========================================

    // ===============
    // Boolean Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<Boolean>> EXEC_OPTIONS_BOOLEAN_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<Boolean>>()
                    .build();

    // ===============
    // String Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<String>> EXEC_OPTIONS_STRING_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<String>>()
                    .put(SQL_OPTIMIZER_PLAN_PUT_CACHE.key(), SQL_OPTIMIZER_PLAN_PUT_CACHE)
                    .put(SQL_OPTIMIZER_PLAN_GET_CACHE.key(), SQL_OPTIMIZER_PLAN_GET_CACHE)
                    .put(EXEC_OPTIMIZER_STREAMING_BATCH_SIZE.key(), EXEC_OPTIMIZER_STREAMING_BATCH_SIZE)
                    .put(SQL_OPTIMIZER_SQL_PARSE_TIME_US.key(), SQL_OPTIMIZER_SQL_PARSE_TIME_US)
                    .put(SQL_OPTIMIZER_CATALOG_REGISTER_TIME_US.key(), SQL_OPTIMIZER_CATALOG_REGISTER_TIME_US)
                    .put(SQL_OPTIMIZER_SQL_VALIDATE_TIME_US.key(), SQL_OPTIMIZER_SQL_VALIDATE_TIME_US)
                    .put(SQL_OPTIMIZER_REL_TRANSFORM_TIME_US.key(), SQL_OPTIMIZER_REL_TRANSFORM_TIME_US)
                    .put(SQL_OPTIMIZER_REL_OPTIMIZE_TIME_US.key(), SQL_OPTIMIZER_REL_OPTIMIZE_TIME_US)
                    .put(SQL_OPTIMIZER_REL_POST_OPTIMIZE_TIME_US.key(), SQL_OPTIMIZER_REL_POST_OPTIMIZE_TIME_US)
                    .build();

    // ===============
    // Integer Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<Integer>> EXEC_OPTIONS_INTEGER_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<Integer>>()
                    .build();

    // ===============
    // Double Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<Double>> EXEC_OPTIONS_DOUBLE_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<Double>>()
                    .build();


    // ===========================================
    // Utils
    // ===========================================
    public static void addToMap(IquanConfigManager config, Map<String, Object> execParams) {
        // Boolean Options
        for (Map.Entry<String, IquanConfiguration<Boolean>> entry : EXEC_OPTIONS_BOOLEAN_KEY_MAP.entrySet()) {
            String key = entry.getKey();
            if (config.containsKey(key)) {
                execParams.put(key, config.getBoolean(entry.getValue()));
            }
        }

        // String Options
        for (Map.Entry<String, IquanConfiguration<String>> entry : EXEC_OPTIONS_STRING_KEY_MAP.entrySet()) {
            String key = entry.getKey();
            if (config.containsKey(key)) {
                execParams.put(key, config.getString(entry.getValue()));
            }
        }

        // Integer Options
        for (Map.Entry<String, IquanConfiguration<Integer>> entry : EXEC_OPTIONS_INTEGER_KEY_MAP.entrySet()) {
            String key = entry.getKey();
            if (config.containsKey(key)) {
                execParams.put(key, config.getInteger(entry.getValue()));
            }
        }

        // Double Options
        for (Map.Entry<String, IquanConfiguration<Double>> entry : EXEC_OPTIONS_DOUBLE_KEY_MAP.entrySet()) {
            String key = entry.getKey();
            if (config.containsKey(key)) {
                execParams.put(key, config.getDouble(entry.getValue()));
            }
        }
    }
}
