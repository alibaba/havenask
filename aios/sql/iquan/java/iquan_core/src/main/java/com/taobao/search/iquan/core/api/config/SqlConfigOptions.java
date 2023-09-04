package com.taobao.search.iquan.core.api.config;

import com.google.common.collect.ImmutableMap;
import com.taobao.search.iquan.core.api.common.*;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;

import java.util.Map;

public class SqlConfigOptions {

    // ===========================================
    // Options
    // ===========================================
    public static final IquanConfiguration<Boolean> IQUAN_USE_DEFAULT_QRS =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.use.default.qrs")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("ignore registered compute node and use default qrs as single compute node")
                    .build();
    public static final IquanConfiguration<String> IQUAN_OPTIMIZER_LEVEL =
            IquanConfiguration.<String>builder()
                    .key("iquan.optimizer.level")
                    .clazz(String.class)
                    .defaultValue(OptimizeLevel.Os.getLevel())
                    .description("optimizer level like gcc")
                    .build();

    public static final IquanConfiguration<Integer> IQUAN_CTE_OPT_VER =
            IquanConfiguration.<Integer>builder()
                    .key("iquan.cte.opt.ver")
                    .clazz(Integer.class)
                    .defaultValue(1)
                    .description("cte optimize version for backward compatibility")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_DEBUG_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.debug.enable")
                    .clazz(Boolean.class)
                    .defaultValue(false)
                    .description("enable debug of iquan optimizer.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_DETAIL_DEBUG_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.detail.debug.enable")
                    .clazz(Boolean.class)
                    .defaultValue(false)
                    .description("enable detail debug of iquan optimizer.")
                    .build();

    public static final IquanConfiguration<String> IQUAN_OPTIMIZER_RULE_GROUP =
            IquanConfiguration.<String>builder()
                    .key("iquan.optimizer.rule.group")
                    .clazz(String.class)
                    .defaultValue(ConstantDefine.DEFAULT)
                    .description("set optimize rule group.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_SORT_LIMIT_USE_TOGETHER =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.sort.limit.use.together")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("enable sort limit must use together.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_FORCE_LIMIT_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.force.limit.enable")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("enable force limit.")
                    .build();

    public static final IquanConfiguration<Integer> IQUAN_OPTIMIZER_FORCE_LIMIT_NUM =
            IquanConfiguration.<Integer>builder()
                    .key("iquan.optimizer.force.limit.num")
                    .clazz(Integer.class)
                    .defaultValue(100)
                    .description("set force limit num.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_JOIN_CONDITION_CHECK =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.join.condition.check")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("set join condition check.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_FORCE_HASH_JOIN =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.force.hash.join")
                    .clazz(Boolean.class)
                    .defaultValue(false)
                    .description("set force hash join.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_TURBOJET_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.turbojet.enable")
                    .clazz(Boolean.class)
                    .defaultValue(false)
                    .description("toggle turbojet plan mode.")
                    .build();

    public static final IquanConfiguration<String> IQUAN_PLAN_FORMAT_VERSION =
            IquanConfiguration.<String>builder()
                    .key("iquan.plan.format.version")
                    .clazz(String.class)
                    .defaultValue(ConstantDefine.EMPTY)
                    .description("set iquan output plan format version.")
                    .build();

    public static final IquanConfiguration<String> IQUAN_PLAN_FORMAT_TYPE =
            IquanConfiguration.<String>builder()
                    .key("iquan.plan.format.type")
                    .clazz(String.class)
                    .defaultValue(PlanFormatType.JSON.getType())
                    .description("set iquan output plan format type.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_PLAN_FORMAT_OBJECT_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.plan.format.object.enable")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("enable output json object instead of json string.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_PLAN_OUTPUT_EXEC_PARAMS =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.plan.output.exec_params")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("enable output exec params.")
                    .build();

    public static final IquanConfiguration<String> IQUAN_PLAN_PREPARE_LEVEL =
            IquanConfiguration.<String>builder()
                    .key("iquan.plan.prepare.level")
                    .clazz(String.class)
                    .defaultValue(SqlExecPhase.JNI_POST_OPTIMIZE.getName())
                    .description("set plan prepare level.")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_PLAN_CACHE_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.plan.cache.enable")
                    .clazz(Boolean.class)
                    .defaultValue(false)
                    .description("enable plan cache.")
                    .build();

    public static final IquanConfiguration<String> IQUAN_PLAN_EXEC_PHASE_RESULT =
            IquanConfiguration.<String>builder()
                    .key("iquan.plan.exec.phase.result")
                    .clazz(String.class)
                    .defaultValue(SqlExecPhase.END.getName())
                    .description("return specified phase result.")
                    .build();

    public static final IquanConfiguration<String> IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX =
            IquanConfiguration.<String>builder()
                    .key("iquan.optimizer.table.summary.suffix")
                    .clazz(String.class)
                    .defaultValue(ConstantDefine.TABLE_SUMMARY_SUFFIX)
                    .description("set summary table name suffix")
                    .build();

    public static final IquanConfiguration<Integer> IQUAN_FB_INIT_SIZE =
            IquanConfiguration.<Integer>builder()
                    .key("iquan.fb.init.size")
                    .clazz(Integer.class)
                    .defaultValue(4 * 1024)
                    .description("init size of fb builder")
                    .build();

    public static final IquanConfiguration<String> IQUAN_DEFAULT_CATALOG_NAME =
            IquanConfiguration.<String>builder()
                    .key("iquan.default.catalog.name")
                    .clazz(String.class)
                    .defaultValue("default")
                    .description("defaut catlog name")
                    .build();

    public static final IquanConfiguration<String> IQUAN_DEFAULT_DB_NAME =
            IquanConfiguration.<String>builder()
                    .key("iquan.default.db.name")
                    .clazz(String.class)
                    .defaultValue("default")
                    .description("defaut db name")
                    .build();

    public static final IquanConfiguration<Boolean> IQUAN_OPTIMIZER_CTE_ENABLE =
            IquanConfiguration.<Boolean>builder()
                    .key("iquan.optimizer.cte.enable")
                    .clazz(Boolean.class)
                    .defaultValue(true)
                    .description("enable cte optimize.")
                    .build();
    

    // ===========================================
    // Note: should contain all option keys
    // ===========================================

    // ===============
    // Boolean Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<Boolean>> IQUAN_OPTIONS_BOOLEAN_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<Boolean>>()
                    .put(IQUAN_OPTIMIZER_DEBUG_ENABLE.key(), IQUAN_OPTIMIZER_DEBUG_ENABLE)
                    .put(IQUAN_OPTIMIZER_DETAIL_DEBUG_ENABLE.key(), IQUAN_OPTIMIZER_DETAIL_DEBUG_ENABLE)
                    .put(IQUAN_OPTIMIZER_SORT_LIMIT_USE_TOGETHER.key(), IQUAN_OPTIMIZER_SORT_LIMIT_USE_TOGETHER)
                    .put(IQUAN_OPTIMIZER_FORCE_LIMIT_ENABLE.key(), IQUAN_OPTIMIZER_FORCE_LIMIT_ENABLE)
                    .put(IQUAN_OPTIMIZER_JOIN_CONDITION_CHECK.key(), IQUAN_OPTIMIZER_JOIN_CONDITION_CHECK)
                    .put(IQUAN_OPTIMIZER_FORCE_HASH_JOIN.key(), IQUAN_OPTIMIZER_FORCE_HASH_JOIN)
                    .put(IQUAN_PLAN_FORMAT_OBJECT_ENABLE.key(), IQUAN_PLAN_FORMAT_OBJECT_ENABLE)
                    .put(IQUAN_PLAN_OUTPUT_EXEC_PARAMS.key(), IQUAN_PLAN_OUTPUT_EXEC_PARAMS)
                    .put(IQUAN_PLAN_CACHE_ENABLE.key(), IQUAN_PLAN_CACHE_ENABLE)
                    .put(IQUAN_OPTIMIZER_CTE_ENABLE.key(), IQUAN_OPTIMIZER_CTE_ENABLE)
                    .put(IQUAN_USE_DEFAULT_QRS.key(), IQUAN_USE_DEFAULT_QRS)
                    .build();

    // ===============
    // String Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<String>> IQUAN_OPTIONS_STRING_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<String>>()
                    .put(IQUAN_OPTIMIZER_RULE_GROUP.key(), IQUAN_OPTIMIZER_RULE_GROUP)
                    .put(IQUAN_PLAN_FORMAT_VERSION.key(), IQUAN_PLAN_FORMAT_VERSION)
                    .put(IQUAN_PLAN_FORMAT_TYPE.key(), IQUAN_PLAN_FORMAT_TYPE)
                    .put(IQUAN_PLAN_PREPARE_LEVEL.key(), IQUAN_PLAN_PREPARE_LEVEL)
                    .put(IQUAN_PLAN_EXEC_PHASE_RESULT.key(), IQUAN_PLAN_EXEC_PHASE_RESULT)
                    .put(IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX.key(), IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX)
                    .put(IQUAN_DEFAULT_CATALOG_NAME.key(), IQUAN_DEFAULT_CATALOG_NAME)
                    .put(IQUAN_DEFAULT_DB_NAME.key(), IQUAN_DEFAULT_DB_NAME)
                    .put(IQUAN_OPTIMIZER_LEVEL.key(), IQUAN_OPTIMIZER_LEVEL)
                    .build();

    // ===============
    // Integer Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<Integer>> IQUAN_OPTIONS_INTEGER_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<Integer>>()
                    .put(IQUAN_OPTIMIZER_FORCE_LIMIT_NUM.key(), IQUAN_OPTIMIZER_FORCE_LIMIT_NUM)
                    .put(IQUAN_FB_INIT_SIZE.key(), IQUAN_FB_INIT_SIZE)
                    .build();

    // ===============
    // Double Options
    // ===============
    public static final ImmutableMap<String, IquanConfiguration<Double>> IQUAN_OPTIONS_DOUBLE_KEY_MAP =
            new ImmutableMap.Builder<String, IquanConfiguration<Double>>()
                    .build();


    // ===========================================
    // Utils
    // ===========================================
    public static void addToConfiguration(Map<String, Object> sqlParams, IquanConfigManager configManager) {
        for (Map.Entry<String, Object> entry : sqlParams.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key == null || value == null) {
                continue;
            }

            if (IQUAN_OPTIONS_BOOLEAN_KEY_MAP.containsKey(key)) {
                // Boolean Options
                if (value instanceof Boolean) {
                    configManager.setBoolean(IQUAN_OPTIONS_BOOLEAN_KEY_MAP.get(key), (boolean) value);
                } else if (value instanceof String) {
                    configManager.setBoolean(IQUAN_OPTIONS_BOOLEAN_KEY_MAP.get(key), Boolean.parseBoolean((String) value));
                } else {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CONF_UNSUPPORT_KEY,
                            String.format("boolean param is %s=%s", key, value.toString()));
                }

            } else if (IQUAN_OPTIONS_STRING_KEY_MAP.containsKey(key)) {
                // String Options
                if (value instanceof String) {
                    configManager.setString(IQUAN_OPTIONS_STRING_KEY_MAP.get(key), (String) value);
                } else {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CONF_UNSUPPORT_KEY,
                            String.format("string param is %s=%s", key, value.toString()));
                }

            } else if (IQUAN_OPTIONS_INTEGER_KEY_MAP.containsKey(key)) {
                // Integer Options
                if (value instanceof Integer) {
                    configManager.setInteger(IQUAN_OPTIONS_INTEGER_KEY_MAP.get(key), (int) value);
                } else if (value instanceof String) {
                    configManager.setInteger(IQUAN_OPTIONS_INTEGER_KEY_MAP.get(key), Integer.parseInt((String) value));
                } else {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CONF_UNSUPPORT_KEY,
                            String.format("integer param is %s=%s", key, value.toString()));
                }

            } else if (IQUAN_OPTIONS_DOUBLE_KEY_MAP.containsKey(key)) {
                // Double Options
                if (value instanceof Double) {
                    configManager.setDouble(IQUAN_OPTIONS_DOUBLE_KEY_MAP.get(key), (double) value);
                } else if (value instanceof String) {
                    configManager.setDouble(IQUAN_OPTIONS_DOUBLE_KEY_MAP.get(key), Double.parseDouble((String) value));
                } else {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CONF_UNSUPPORT_KEY,
                            String.format("double param is %s=%s", key, value.toString()));
                }

            } else {
                configManager.set(
                        IquanConfiguration.<String>builder().key(key).clazz(String.class).description("").build(),
                        value.toString());
            }
        }
    }

    public static String getConfigurationStr(IquanConfigManager configManager) {
        StringBuilder sb = new StringBuilder(512);

        // Boolean Options
        for (String key : IQUAN_OPTIONS_BOOLEAN_KEY_MAP.keySet()) {
            boolean value = configManager.getBoolean(IQUAN_OPTIONS_BOOLEAN_KEY_MAP.get(key));
            sb.append(value).append(ConstantDefine.SEPERATOR);
        }

        // String Options
        for (String key : IQUAN_OPTIONS_STRING_KEY_MAP.keySet()) {
            String value = configManager.getString(IQUAN_OPTIONS_STRING_KEY_MAP.get(key));
            sb.append(value).append(ConstantDefine.SEPERATOR);
        }

        // Integer Options
        for (String key : IQUAN_OPTIONS_INTEGER_KEY_MAP.keySet()) {
            int value = configManager.getInteger(IQUAN_OPTIONS_INTEGER_KEY_MAP.get(key));
            sb.append(value).append(ConstantDefine.SEPERATOR);
        }

        // Double Options
        for (String key : IQUAN_OPTIONS_DOUBLE_KEY_MAP.keySet()) {
            double value = configManager.getDouble(IQUAN_OPTIONS_DOUBLE_KEY_MAP.get(key));
            sb.append(value).append(ConstantDefine.SEPERATOR);
        }

        return sb.toString();
    }
}
