package com.taobao.search.iquan.core.rel.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.taobao.search.iquan.core.api.common.PlanFormatVersion;
import com.taobao.search.iquan.core.api.exception.PlanWriteException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanIdentityOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

/**
 * Callback for a relational expression to dump itself as JSON.
 * <p>
 * Copy from org.apache.calcite.rel.externalize.RelJsonWriter, minor modify
 *
 * @see RelJsonReader
 */
public class IquanJsonWriter implements RelWriter {
    //~ Instance fields ----------------------------------------------------------
    private final Map<RelNode, Integer> relIdMap = new IdentityHashMap<>();
    private final List<Object> relList = new ArrayList<>();
    private final Map<Integer, List<Integer>> inputIdsMap = new HashMap<>();
    private final Map<Integer, List<Integer>> outputIdsMap = new HashMap<>();
    private final List<Pair<String, Object>> values = new ArrayList<>();
    private Stack<String> scopes = new Stack<>();

    //~ Constructors -------------------------------------------------------------

    public IquanJsonWriter() {
        scopes.push("default");
    }

    //~ Methods ------------------------------------------------------------------

    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        final Map<String, Object> map = new TreeMap<>();
        List<String> inputNameList;

        map.put(ConstantDefine.ID, ConstantDefine.EMPTY); // ensure that id is the first attribute
        boolean isIdentity = false;
        if (rel instanceof IquanRelNode) {
            IquanRelNode iquanRelNode = (IquanRelNode) rel;
            if (iquanRelNode instanceof IquanIdentityOp) {
                isIdentity = true;
                IquanIdentityOp identity = (IquanIdentityOp) iquanRelNode;
                scopes.push(identity.getNodeName());
            }
            map.put(ConstantDefine.OP_NAME, iquanRelNode.getName());
            inputNameList = iquanRelNode.getInputNames();
        } else {
            map.put(ConstantDefine.OP_NAME, rel.getClass().getSimpleName());
            inputNameList = ImmutableList.of(ConstantDefine.INPUT);
        }
        explainAttrs(rel, values, map);

        // format inputs
        final List<Integer> inputIdList = explainInputIds(rel.getInputs());
        Map<String, List<Integer>> inputNameIdsMap = explainInputNameIdsMap(inputNameList, inputIdList);
        map.put(ConstantDefine.INPUTS, inputNameIdsMap);
        if (isIdentity) {
            scopes.pop();
        }

        Integer id = relIdMap.size();
        map.put(ConstantDefine.ID, id);

        // calc input and output edges
        inputIdsMap.put(id, inputIdList);
        for (Integer inputId : inputIdList) {
            if (!outputIdsMap.containsKey(inputId)) {
                outputIdsMap.put(inputId, new ArrayList<>());
            }
            outputIdsMap.get(inputId).add(id);
        }

        //
        relIdMap.put(rel, id);
        relList.add(map);
    }

    private void addOpScope(List<Pair<String, Object>> values) {
        for (Pair<String, Object> value : values) {
            if (!ConstantDefine.ATTRS.equals(value.left)) {
                continue;
            }
            if (!(value.right instanceof Map)) {
                continue;
            }
            Map<String, Object> map = (Map<String, Object>) value.right;
            map.put(ConstantDefine.OP_SCOPE, scopes.peek());
        }
    }

    private void put(Map<String, Object> map, String name, Object value) {
        map.put(name, value);
    }

    protected void explainAttrs(RelNode rel, List<Pair<String, Object>> values, Map<String, Object> map) {
        addOpScope(values);
        for (Pair<String, Object> value : values) {
            if (!(value.right instanceof String) && !(value.right instanceof Map)) {
                continue;
            }
            put(map, value.left, value.right);
        }
    }

    private List<Integer> explainInputIds(List<RelNode> inputs) {
        final List<Integer> list = new ArrayList<>();
        for (RelNode input : inputs) {
            Integer id = relIdMap.get(input);
            if (id == null) {
                input.explain(this);
                id = relIdMap.get(input);
            }
            if (id == null) {
                throw new PlanWriteException("Get input failed!");
            }
            list.add(id);
        }
        return list;
    }

    private Map<String, List<Integer>> explainInputNameIdsMap(List<String> inputNames, List<Integer> inputIds) {
        if (inputNames.isEmpty()) {
            throw new PlanWriteException("Inputs names is empty!");
        }

        Map<String, List<Integer>> inputNameIdsMap = new TreeMap<>();
        int minSize = Math.min(inputNames.size(), inputIds.size());
        for (int i = 0; i < minSize; ++i) {
            inputNameIdsMap.put(inputNames.get(i), Lists.newArrayList(inputIds.get(i)));
        }

        if (inputNames.size() > minSize) {
            for (int j = minSize; j < inputNames.size(); ++j) {
                inputNameIdsMap.put(inputNames.get(j), new ArrayList<>());
            }
        } else if (inputIds.size() > minSize) {
            String lastName = inputNames.get(inputNames.size() - 1);
            List<Integer> lastNameIds = inputNameIdsMap.get(lastName);
            for (int j = minSize; j < inputIds.size(); ++j) {
                lastNameIds.add(inputIds.get(j));
            }
        }

        return inputNameIdsMap;
    }

    public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        explain_(rel, valueList);
    }

    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    public RelWriter input(String term, RelNode input) {
        return this;
    }

    public RelWriter item(String term, Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    @SuppressWarnings("unchecked")
    private List<Object> getList(List<Pair<String, Object>> values, String tag) {
        for (Pair<String, Object> value : values) {
            if (value.left.equals(tag)) {
                //noinspection unchecked
                return (List<Object>) value.right;
            }
        }
        final List<Object> list = new ArrayList<>();
        values.add(Pair.of(tag, list));
        return list;
    }

    public RelWriter itemIf(String term, Object value, boolean condition) {
        if (condition) {
            item(term, value);
        }
        return this;
    }

    public RelWriter done(RelNode node) {
        final List<Pair<String, Object>> valuesCopy =
                ImmutableList.copyOf(values);
        values.clear();
        explain_(node, valuesCopy);
        return this;
    }

    public boolean nest() {
        return true;
    }

    /**
     * Returns a JSON string describing the relational expressions that were just
     * explained.
     *
     * @return String
     */
    public String asString(Map<String, Object> execParams, Map<String, List<Object>> optimizeInfos, Map<String, Object> planMeta, PlanFormatVersion formatVersion) {
        return IquanRelOptUtils.toJson(asObject(execParams, optimizeInfos, planMeta, formatVersion));
    }

    /**
     * Returns a Pretty JSON string describing the relational expressions that were just
     * explained.
     *
     * @return String
     */
    public String asPrettyString(Map<String, Object> execParams,
                                 Map<String, List<Object>> optimizeInfos,
                                 Map<String, Object> planMeta,
                                 PlanFormatVersion formatVersion) {
        return IquanRelOptUtils.toPrettyJson(asObject(execParams, optimizeInfos, planMeta, formatVersion));
    }


    @SuppressWarnings("unchecked")
    public Object asObject(Map<String, Object> execParams, Map<String, List<Object>> optimizeInfos, Map<String, Object> planMeta, PlanFormatVersion formatVersion) {
        for (int id = 0; id < relList.size(); ++id) {
            Map<String, Object> relOp = (Map<String, Object>) relList.get(id);

            if (outputIdsMap.containsKey(id)) {
                relOp.put(ConstantDefine.OUTPUTS, outputIdsMap.get(id));
            }

            List<Integer> reuseInputs = new ArrayList<>();
            List<Integer> inputIdList = inputIdsMap.containsKey(id) ? inputIdsMap.get(id) : new ArrayList<>();
            for (int inputId : inputIdList) {
                if (outputIdsMap.get(inputId).size() > 1) {
                    reuseInputs.add(inputId);
                }
            }
            if (!reuseInputs.isEmpty()) {
                relOp.put(ConstantDefine.REUSE_INPUTS, reuseInputs);
                // TODO: need remove when navi support
                if (relOp.containsKey(ConstantDefine.ATTRS)) {
                    Object attrsObj = relOp.get(ConstantDefine.ATTRS);
                    if (attrsObj instanceof Map) {
                        Map<String, Object> attrsMap = (Map<String, Object>) attrsObj;
                        attrsMap.put(ConstantDefine.REUSE_INPUTS, reuseInputs);
                    }
                }
            }
        }

        final Map<String, Object> map = new TreeMap<>();
        map.put(ConstantDefine.REL_PLAN_KEY, relList);
        map.put(ConstantDefine.REL_PLAN_VERSION_KEY, formatVersion.getVersion());
        if (execParams != null) {
            map.put(ConstantDefine.EXEC_PARAMS_KEY, execParams);
        }
        if (optimizeInfos != null) {
            map.put(ConstantDefine.OPTIMIZE_INFOS, optimizeInfos);
        }
        if (planMeta != null) {
            map.put(ConstantDefine.PLAN_META, planMeta);
        }
        return map;
    }
}

// End RelJsonWriter.java

