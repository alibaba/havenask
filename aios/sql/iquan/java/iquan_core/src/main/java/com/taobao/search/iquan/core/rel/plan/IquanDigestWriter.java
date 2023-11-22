package com.taobao.search.iquan.core.rel.plan;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.exception.PlanWriteException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
public class IquanDigestWriter implements RelWriter {
    //~ Instance fields ----------------------------------------------------------
    private final Map<RelNode, Integer> relIdMap = new IdentityHashMap<>();
    private final List<Pair<String, Object>> values = new ArrayList<>();
    private final List<String> outputs;
    private final SqlExplainLevel explainLevel;
    private final boolean withRelId;
    private int depth = 0;

    //~ Constructors -------------------------------------------------------------

    public IquanDigestWriter(SqlExplainLevel level) {
        this(level, true);
    }

    public IquanDigestWriter(SqlExplainLevel level, boolean withRelId) {
        this.withRelId = withRelId;
        outputs = new ArrayList<>();
        outputs.add(ConstantDefine.EMPTY);
        explainLevel = level;
    }

    //~ Methods ------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) throws Exception {
        int index = outputs.size();
        outputs.add("");

        depth += 1;
        for (RelNode input : rel.getInputs()) {
            input.explain(this);
        }
        depth -= 1;

        StringBuilder sb = new StringBuilder(256);
        for (int i = 0; i < depth * 4; ++i) {
            sb.append(ConstantDefine.SPACE);
        }

        sb.append(IquanRelOptUtils.getRelNodeName(rel));
        if (withRelId) {
            Integer id = relIdMap.get(rel);
            if (id == null) {
                id = relIdMap.size();
                relIdMap.put(rel, id);
            }
            sb.append(ConstantDefine.TABLE_SEPARATOR);
            sb.append(id);
        }
        sb.append(ConstantDefine.LEFT_PARENTHESIS);

        String content;
        if (rel instanceof IquanRelNode) {
            Map<String, Object> attrMap = getAttrMap(values);
            content = explainAttrMap(attrMap);
        } else {
            content = explainDefault(values);
        }
        sb.append(content);
        sb.append(ConstantDefine.RIGHT_PARENTHESIS);
        outputs.set(index, sb.toString());
    }

    public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        try {
            explain_(rel, valueList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public SqlExplainLevel getDetailLevel() {
        return explainLevel;
    }

    public RelWriter input(String term, RelNode input) {
        return this;
    }

    @Override
    public RelWriter item(String term, Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    @Override
    public RelWriter itemIf(String s, Object o, boolean b) {
        if (b) {
            item(s, o);
        }
        return this;
    }

    public RelWriter done(RelNode node) {
        final List<Pair<String, Object>> valuesCopy =
                ImmutableList.copyOf(values);
        values.clear();
        try {
            explain_(node, valuesCopy);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public boolean nest() {
        return true;
    }

    /**
     * Returns a JSON string describing the relational expressions that were just
     * explained.
     *
     * @return String
     */
    public String asString() {
        outputs.add(ConstantDefine.EMPTY);
        return String.join(ConstantDefine.NEW_LINE, outputs);
    }

    public Object asObject() {
        return asString();
    }

    //================================================================
    // Internal Format Function
    //================================================================
    @SuppressWarnings("unchecked")
    private Map<String, Object> getAttrMap(List<Pair<String, Object>> values) {
        try {
            for (Pair<String, Object> pair : values) {
                if (pair.left.equals(ConstantDefine.ATTRS)) {
                    if (pair.right instanceof Map) {
                        return (Map<String, Object>) pair.right;
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new PlanWriteException(String.format("Get %s failed.", ConstantDefine.ATTRS));
        }
    }

    private String explainAttrMap(Map<String, Object> attrs) {
        if (attrs == null) {
            return "";
        }
        List<String> outputs = new ArrayList<>();
        for (String key : attrs.keySet()) {
            if (key.equals(ConstantDefine.CONDITION)) {
                IquanRelOptUtils.addListIfNotEmpty(outputs, formatCondition(attrs, key));
            } else if (key.equals(ConstantDefine.EQUI_CONDITION)) {
                IquanRelOptUtils.addListIfNotEmpty(outputs, formatCondition(attrs, key));
            } else if (key.equals(ConstantDefine.REMAINING_CONDITION)) {
                IquanRelOptUtils.addListIfNotEmpty(outputs, formatCondition(attrs, key));
            } else if (key.equals(ConstantDefine.OUTPUT_FIELD_EXPRS)) {
                IquanRelOptUtils.addListIfNotEmpty(outputs, formatOutputFieldExprs(attrs));
            } else if (key.equals(ConstantDefine.NEST_TABLE_ATTRS)) {
                IquanRelOptUtils.addListIfNotEmpty(outputs, formatNestTableAttrs(attrs));
            } else {
                IquanRelOptUtils.addListIfNotEmpty(outputs, formatObject(attrs, key));
            }
        }
        return String.join(ConstantDefine.COMMA, outputs);
    }

    private String explainDefault(List<Pair<String, Object>> values) {
        List<String> outputs = new ArrayList<>();
        for (Pair<String, Object> pair : values) {
            IquanRelOptUtils.addListIfNotEmpty(outputs, formatObject(pair.left, pair.right));
        }
        return String.join(ConstantDefine.COMMA, outputs);
    }

    private String formatObject(String key, Object value) {
        if (IquanRelOptUtils.isEmpty(value)) {
            return null;
        }
        return key + ConstantDefine.EQUAL + formatObject(value);
    }

    private String formatObject(Map<String, Object> attrs, String attrName) {
        if (!attrs.containsKey(attrName)) {
            return null;
        }
        Object value = attrs.get(attrName);
        return formatObject(attrName, value);
    }

    @SuppressWarnings("unchecked")
    private String formatObject(Object object) {
        if (object == null) {
            throw new PlanWriteException(String.format("Format object failed, object is null."));
        }
        if (object instanceof Boolean || object instanceof Number || object instanceof String) {
            return object.toString();
        } else if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            List<String> newList = new ArrayList<>();
            list.stream().forEach(v ->
                    IquanRelOptUtils.addListIfNotEmpty(newList, formatObject(v))
            );
            return ConstantDefine.LEFT_BRACKET
                    + String.join(ConstantDefine.COMMA, newList)
                    + ConstantDefine.RIGHT_BRACKET;
        } else if (object instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) object;
            List<String> newList = new ArrayList<>();
            map.keySet().stream().forEach(k ->
                    IquanRelOptUtils.addListIfNotEmpty(newList, k + ConstantDefine.COLON + formatObject(map.get(k)))
            );
            return ConstantDefine.LEFT_BIG_PARENTHESIS
                    + String.join(ConstantDefine.COMMA, newList)
                    + ConstantDefine.RIGHT_BIG_PARENTHESIS;
        } else {
            return object.toString();
        }
    }

    private String formatCondition(Map<String, Object> attrs, String key) {
        if (!attrs.containsKey(key)) {
            return null;
        }
        Object value = attrs.get(key);
        if (!(value instanceof Map)) {
            return key + ConstantDefine.EQUAL + value.toString();
        }
        return key + ConstantDefine.EQUAL + formatCondition(value);
    }

    @SuppressWarnings("unchecked")
    private String formatRexCall(Map<String, Object> map) {
        try {
            Object opName = map.get(ConstantDefine.OP);
            List<Object> params = (List<Object>) map.get(ConstantDefine.PARAMS);

            StringBuilder sb = new StringBuilder(128);
            sb.append(opName);
            sb.append(ConstantDefine.LEFT_PARENTHESIS);
            List<String> newList = new ArrayList<>();
            if (params != null) {
                params.stream().forEach(v -> IquanRelOptUtils.addListIfNotEmpty(newList, formatCondition(v)));
            }
            sb.append(String.join(ConstantDefine.COMMA, newList));
            sb.append(ConstantDefine.RIGHT_PARENTHESIS);
            return sb.toString();
        } catch (Exception ex) {
            throw new PlanWriteException(String.format("Format RexCall %s falied.", map.get(ConstantDefine.OP)));
        }
    }

    @SuppressWarnings("unchecked")
    private String formatRexFieldAccess(Map<String, Object> map) {
        try {
            Object opName = map.get(ConstantDefine.OP);
            Object fieldName = map.get(ConstantDefine.FIELD_NAME);
            Object fieldType = map.get(ConstantDefine.FIELD_TYPE);
            List<Object> params = (List<Object>) map.get(ConstantDefine.PARAMS);
            List<String> paramTypes = (List<String>) map.get(ConstantDefine.PARAM_TYPES);

            StringBuilder sb = new StringBuilder(256);
            sb.append(opName);
            sb.append(ConstantDefine.LEFT_PARENTHESIS);
            sb.append(fieldName).append(ConstantDefine.COMMA).append(fieldType);
            if (paramTypes != null && !paramTypes.isEmpty()) {
                sb.append(ConstantDefine.COMMA).append(paramTypes.get(0));
            }
            if (params != null && !params.isEmpty()) {
                sb.append(ConstantDefine.COMMA).append(formatCondition(params.get(0)));
            }
            sb.append(ConstantDefine.RIGHT_PARENTHESIS);
            return sb.toString();
        } catch (Exception ex) {
            throw new PlanWriteException(String.format("Format RexFieldAccess %s falied.", map.get(ConstantDefine.FIELD_NAME)));
        }
    }

    @SuppressWarnings("unchecked")
    private String formatCondition(Object value) {
        if (value == null) {
            return "null";
        } else if (!(value instanceof Map)) {
            return value.toString();
        }

        Map<String, Object> map = (Map<String, Object>) value;
        String type = (String) map.get(ConstantDefine.OP);
        if (ConstantDefine.REX_ACCESS_FIELD.equals(type)) {
            return formatRexFieldAccess(map);
        } else {
            return formatRexCall(map);
        }
    }

    private String formatOutputFieldExprs(Map<String, Object> attrs) {
        if (!attrs.containsKey(ConstantDefine.OUTPUT_FIELD_EXPRS)) {
            return null;
        }
        Object value = attrs.get(ConstantDefine.OUTPUT_FIELD_EXPRS);
        if (!(value instanceof Map)) {
            return null;
        }
        return ConstantDefine.OUTPUT_FIELD_EXPRS + ConstantDefine.EQUAL + formatOutputFieldExprs(value);
    }

    @SuppressWarnings("unchecked")
    private String formatOutputFieldExprs(Object value) {
        try {
            Map<String, Object> map = (Map<String, Object>) value;
            List<String> newList = new ArrayList<>();
            for (String key : map.keySet()) {
                IquanRelOptUtils.addListIfNotEmpty(newList, key + ConstantDefine.COLON + formatCondition(map.get(key)));
            }
            return ConstantDefine.LEFT_BIG_PARENTHESIS
                    + String.join(ConstantDefine.COMMA, newList)
                    + ConstantDefine.RIGHT_BIG_PARENTHESIS;
        } catch (Exception ex) {
            throw new PlanWriteException(String.format("Format output field exprs(%s) falied.", ConstantDefine.OUTPUT_FIELD_EXPRS));
        }
    }

    private String formatNestTableAttrs(Map<String, Object> attrs) {
        if (!attrs.containsKey(ConstantDefine.NEST_TABLE_ATTRS)) {
            return null;
        }
        Object value = attrs.get(ConstantDefine.NEST_TABLE_ATTRS);
        if (!(value instanceof List)) {
            return null;
        }
        return ConstantDefine.NEST_TABLE_ATTRS + ConstantDefine.EQUAL + formatNestTableAttrs(value);
    }

    @SuppressWarnings("unchecked")
    private String formatNestTableAttrs(Object object) {
        if (object instanceof Map) {
            return ConstantDefine.LEFT_BIG_PARENTHESIS
                    + explainAttrMap((Map<String, Object>) object)
                    + ConstantDefine.RIGHT_BIG_PARENTHESIS;
        }
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            List<String> newList = new ArrayList<>();
            list.stream().forEach(v ->
                    IquanRelOptUtils.addListIfNotEmpty(newList, formatNestTableAttrs(v)));
            return ConstantDefine.LEFT_BRACKET
                    + String.join(ConstantDefine.SEMICOLON, newList)
                    + ConstantDefine.RIGHT_BRACKET;
        }
        throw new PlanWriteException(String.format("Format nest table(%s) falied.", ConstantDefine.NEST_TABLE_ATTRS));
    }
}

// End RelJsonWriter.java

