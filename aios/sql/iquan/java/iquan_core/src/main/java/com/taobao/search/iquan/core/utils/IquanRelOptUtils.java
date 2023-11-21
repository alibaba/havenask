package com.taobao.search.iquan.core.utils;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.common.PlanFormatType;
import com.taobao.search.iquan.core.api.common.PlanFormatVersion;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.PlanWriteException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.catalog.IquanCatalogTable;
import com.taobao.search.iquan.core.catalog.LayerBaseTable;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.plan.IquanDigestWriter;
import com.taobao.search.iquan.core.rel.plan.IquanHintWriter;
import com.taobao.search.iquan.core.rel.plan.IquanJsonWriter;
import com.taobao.search.iquan.core.rel.plan.IquanThumbWriter;
import com.taobao.search.iquan.core.rel.programs.IquanOptContext;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanRelOptUtils {
    public static final ThreadLocal<ObjectMapper> objectMapper = ThreadLocal.withInitial(() -> {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(
                MapperFeature.AUTO_DETECT_FIELDS,
                MapperFeature.AUTO_DETECT_GETTERS,
                MapperFeature.AUTO_DETECT_IS_GETTERS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
        return objectMapper;
    });
    private static final Logger logger = LoggerFactory.getLogger(IquanRelOptUtils.class);

    public static IquanConfigManager getConfigFromRel(RelNode node) {
        Context context = node.getCluster().getPlanner().getContext();
        if (context instanceof IquanOptContext) {
            return ((IquanOptContext) context).getConfiguration();
        }
        return null;
    }

    public static IquanTable getIquanTable(RelOptTable table) {
        if (table instanceof LayerBaseTable) {
            return getIquanTable(((LayerBaseTable) table).getInnerTable());
        }
        RelOptTableImpl optTableImpl = (RelOptTableImpl) table;
        IquanCatalogTable catalogTable = optTableImpl.unwrap(IquanCatalogTable.class);
        if (catalogTable == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG,
                    String.format("RelOptTableImpl [%s] cast to IquanCatalogTable failed", optTableImpl));
        }
        return catalogTable.getTable();
    }

    public static IquanTable getIquanTable(RelNode node) {
        RelNode relNode = toRel(node);
        if (relNode instanceof TableScan) {
            TableScan scan = (TableScan) relNode;
            return getIquanTable(scan.getTable());
        } else {
            return null;
        }
    }

    public static String toJson(Object object) {
        try {
            return objectMapper.get().writeValueAsString(object);
        } catch (Exception ex) {
            throw new PlanWriteException("toJson fail.", ex);
        }
    }

    public static String toPrettyJson(Object object) {
        try {
            return objectMapper.get().writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (Exception ex) {
            throw new PlanWriteException("toPrettyJson fail.", ex);
        }
    }

    public static <T> T fromJson(String content, Class<T> clazz) {
        try {
            return objectMapper.get().readValue(content, clazz);
        } catch (Exception ex) {
            String errMsg = "fromJson fail: " + content;
            logger.warn(errMsg);
            throw new PlanWriteException(errMsg, ex);
        }
    }

    public static <T> T fromJson(File file, Class<T> clazz) {
        try {
            return objectMapper.get().readValue(file, clazz);
        } catch (Exception ex) {
            throw new PlanWriteException("fromJson fail.", ex);
        }
    }

    public static Object toPlan(boolean enableObject,
                                PlanFormatVersion formatVersion,
                                PlanFormatType formatType,
                                List<RelNode> roots,
                                Map<String, Object> execParams,
                                Map<String, List<Object>> optimizeInfos,
                                Map<String, Object> planMeta) {
        if (enableObject) {
            return toObjectPlan(formatVersion, formatType, roots, execParams, optimizeInfos, planMeta);
        } else {
            return toStringPlan(formatVersion, formatType, roots, execParams, optimizeInfos, planMeta);
        }
    }

    public static RelNode toRel(RelNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof HepRelVertex) {
            node = ((HepRelVertex) node).getCurrentRel();
        }
        return node;
    }

    public static String getRelNodeName(RelNode node) {
        if (node instanceof IquanRelNode) {
            return ((IquanRelNode) node).getName();
        } else {
            return node.getClass().getSimpleName();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getValueFromMap(Map<String, Object> map, String key, T defValue) {
        if (!map.containsKey(key)) {
            return defValue;
        }
        try {
            T t = (T) map.get(key);
            return t;
        } catch (Exception ex) {
            logger.error(String.format("Cast from %s to %s failed:",
                            map.get(key).getClass().getSimpleName(),
                            defValue.getClass().getSimpleName()),
                    ex);
            return defValue;
        }
    }

    public static Long getValueFromMap(Map<String, Object> map, String key, Long defValue) {
        if (!map.containsKey(key)) {
            return defValue;
        }
        try {
            return Long.valueOf(map.get(key).toString());
        } catch (Exception ex) {
            logger.error(String.format("Cast from %s to %s failed:",
                            map.get(key).getClass().getSimpleName(),
                            defValue.getClass().getSimpleName()),
                    ex);
            return defValue;
        }
    }

    public static void addListIfNotEmpty(List<String> list, String value) {
        if (value != null && !value.isEmpty()) {
            list.add(value);
        }
    }

    public static void addListIfNotEmpty(List<Object> list, Object value) {
        if (!isEmpty(value)) {
            list.add(value);
        }
    }

    public static void addMapIfNotEmpty(Map<String, Object> map, String key, Object value) {
        if (!isEmpty(value)) {
            map.put(key, value);
        }
    }

    public static boolean isEmpty(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Number || value instanceof Boolean) {
            return false;
        } else if (value instanceof String) {
            return ((String) value).isEmpty();
        } else if (value instanceof List) {
            return ((List<?>) value).isEmpty();
        } else if (value instanceof Map) {
            return ((Map<?, ?>) value).isEmpty();
        } else if (value instanceof Set) {
            return ((Set<?>) value).isEmpty();
        }
        return value.toString().isEmpty();
    }

    public static Map<String, String> toMap(List<String> list) {
        Map<String, String> map = new TreeMap<>();
        for (String v : list) {
            map.put(v, v);
        }
        return map;
    }

    public static String relToString(RelNode rel) {
        return relToString(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false, true, Collections.emptyList());
    }

    public static String relToString(RelNode rel,
                                     SqlExplainLevel detailLevel,
                                     boolean withIdPrefix,
                                     boolean withRowType,
                                     boolean withTreeStyle,
                                     List<RelNode> borders) {
        if (rel == null) {
            return null;
        }

        StringWriter sw = new StringWriter();
        IquanRelTreeWriterImp planWriter = new IquanRelTreeWriterImp(
                new PrintWriter(sw),
                detailLevel,
                withIdPrefix,
                withRowType,
                withTreeStyle,
                borders);
        rel.explain(planWriter);
        return sw.toString();
    }

    // =======================
    // private function
    // =======================
    private static Object toJsonObject(List<RelNode> roots,
                                       Map<String, Object> execParams,
                                       Map<String, List<Object>> optimizeInfos,
                                       Map<String, Object> planMeta,
                                       PlanFormatVersion formatVersion) {
        if (roots == null || roots.isEmpty()) {
            return "{}";
        }

        IquanJsonWriter pw = new IquanJsonWriter();
        for (RelNode root : roots) {
            root.explain(pw);
        }
        return pw.asObject(execParams, optimizeInfos, planMeta, formatVersion);
    }

    private static String toJsonString(List<RelNode> roots,
                                       Map<String, Object> execParams,
                                       Map<String, List<Object>> optimizeInfos,
                                       Map<String, Object> planMeta,
                                       PlanFormatVersion formatVersion) {
        if (roots == null || roots.isEmpty()) {
            return "{}";
        }

        IquanJsonWriter pw = new IquanJsonWriter();
        for (RelNode root : roots) {
            root.explain(pw);
        }
        return pw.asString(execParams, optimizeInfos, planMeta, formatVersion);
    }

    private static Object toDigestObject(List<RelNode> roots) {
        if (roots == null || roots.isEmpty()) {
            return "";
        }

        IquanDigestWriter pw = new IquanDigestWriter(SqlExplainLevel.DIGEST_ATTRIBUTES);
        for (RelNode root : roots) {
            root.explain(pw);
        }
        return pw.asObject();
    }

    private static String toDigestString(List<RelNode> roots) {
        if (roots == null || roots.isEmpty()) {
            return "";
        }

        IquanDigestWriter pw = new IquanDigestWriter(SqlExplainLevel.DIGEST_ATTRIBUTES);
        for (RelNode root : roots) {
            root.explain(pw);
        }
        return pw.asString();
    }

    private static Object toThumbObject(List<RelNode> roots) {
        if (roots == null || roots.isEmpty()) {
            return "";
        }

        IquanThumbWriter pw = new IquanThumbWriter();
        return pw.asObject(roots);
    }

    private static Object toHintObject(List<RelNode> roots) {
        return toHintString(roots);
    }

    private static String toThumbString(List<RelNode> roots) {
        if (roots == null || roots.isEmpty()) {
            return "";
        }

        IquanThumbWriter pw = new IquanThumbWriter();
        return pw.asString(roots);
    }

    private static String toHintString(List<RelNode> roots) {
        StringBuilder result = new StringBuilder();
        IquanHintWriter visitor = new IquanHintWriter();
        result.append(visitor.asString(roots));
        return result.toString();
    }

    private static Object toObjectPlan(PlanFormatVersion formatVersion,
                                       PlanFormatType formatType,
                                       List<RelNode> roots,
                                       Map<String, Object> execParams,
                                       Map<String, List<Object>> optimizeInfos,
                                       Map<String, Object> planMeta) {
        if (formatType == PlanFormatType.JSON) {
            return IquanRelOptUtils.toJsonObject(roots, execParams, optimizeInfos, planMeta, formatVersion);
        } else if (formatType == PlanFormatType.DIGEST) {
            return IquanRelOptUtils.toDigestObject(roots);
        } else if (formatType == PlanFormatType.THUMB) {
            return IquanRelOptUtils.toThumbObject(roots);
        } else if (formatType == PlanFormatType.HINT) {
            return IquanRelOptUtils.toHintObject(roots);
        } else if (formatType == PlanFormatType.OPTIMIZE_INFOS) {
            return optimizeInfos;
        } else {
            throw new PlanWriteException("not support format type: " + formatType.getType());
        }
    }

    private static String toStringPlan(PlanFormatVersion formatVersion,
                                       PlanFormatType formatType,
                                       List<RelNode> roots,
                                       Map<String, Object> execParams,
                                       Map<String, List<Object>> optimizeInfos,
                                       Map<String, Object> planMeta) {
        if (formatType == PlanFormatType.JSON) {
            return IquanRelOptUtils.toJsonString(roots, execParams, optimizeInfos, planMeta, formatVersion);
        } else if (formatType == PlanFormatType.DIGEST) {
            return IquanRelOptUtils.toDigestString(roots);
        } else if (formatType == PlanFormatType.THUMB) {
            return IquanRelOptUtils.toThumbString(roots);
        } else if (formatType == PlanFormatType.HINT) {
            return IquanRelOptUtils.toHintString(roots);
        } else if (formatType == PlanFormatType.OPTIMIZE_INFOS) {
            return toJson(optimizeInfos);
        } else if (formatType == PlanFormatType.PLAN_META) {
            return toPrettyJson(planMeta);
        } else {
            throw new PlanWriteException("not support format type: " + formatType.getType());
        }
    }
}
