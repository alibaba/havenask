package com.taobao.search.iquan.core.utils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class IquanRelTreeWriterImp extends RelWriterImpl {
    private final boolean withRowType;
    private final boolean withTreeStyle;
    private final List<RelNode> borders;
    private List<Boolean> lastChildren = new ArrayList<>();
    private int depth = 0;

    public IquanRelTreeWriterImp(PrintWriter pw,
                                 SqlExplainLevel detailLevel,
                                 boolean withIdPrefix,
                                 boolean withRowType,
                                 boolean withTreeStyle,
                                 List<RelNode> borders) {
        super(pw, detailLevel, withIdPrefix);
        this.withRowType = withRowType;
        this.withTreeStyle = withTreeStyle;
        this.borders = borders;
    }

    public IquanRelTreeWriterImp(PrintWriter pw,
                                 SqlExplainLevel detailLevel,
                                 boolean withIdPrefix) {
        super(pw, detailLevel, withIdPrefix);
        this.withRowType = false;
        this.withTreeStyle = true;
        borders = new ArrayList<>();
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        List<RelNode> inputs = rel.getInputs();
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        if (!mq.isVisibleInExplain(rel, detailLevel)) {
            inputs.forEach(v -> v.explain(this));
            return;
        }

        StringBuilder s = new StringBuilder();
        if (withTreeStyle) {
            if (depth > 0) {
                dropLastElement(lastChildren).forEach(v -> s.append(v ? "   " : ":  "));
                s.append(getLast(lastChildren) ? "+- " : ":- ");
            }
        }

        if (withIdPrefix) {
            s.append(rel.getId()).append(":");
        }

        int borderIndex = borders.indexOf(rel);
        boolean reachBorder = borderIndex >= 0;
        if (reachBorder) {
            s.append("[#").append(borderIndex + 1).append("] ");
        }

        s.append(rel.getRelTypeName());

        List<Pair<String, Object>> printValues = new ArrayList<>();
        if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
            printValues.addAll(values);
        }

        if (!printValues.isEmpty()) {
            int j = 0;
            for (Pair<String, Object> v : printValues) {
                if (v.right instanceof RelNode) {
                    continue;
                }
                s.append(j == 0 ? "(" : ", ");
                    j = j + 1;
                s.append(v.left).append("=[").append(v.right).append("]");
            }
            if (j > 0) {
                s.append(")");
            }
        }

        if (withRowType) {
            s.append(", rowType=[").append(rel.getRowType().toString()).append("]");
        }

        if (detailLevel == SqlExplainLevel.ALL_ATTRIBUTES) {
            s.append(": rowcount = ")
                    .append(mq.getRowCount(rel))
                    .append(", cumulative cost = ")
                    .append(mq.getCumulativeCost(rel));
        }
        pw.println(s);
        if (reachBorder) {
            return;
        }

        if (inputs.size() > 1) {
            dropLastElement(inputs).forEach(v -> {
                if (withTreeStyle) {
                    ++depth;
                    lastChildren.add(false);
                    v.explain(this);
                    --depth;
                    lastChildren = dropLastElement(lastChildren);
                } else {
                    v.explain(this);
                }
            });
        }

        if (!inputs.isEmpty()) {
            if (withTreeStyle) {
                ++depth;
                lastChildren.add(true);
                getLast(inputs).explain(this);
                --depth;
                lastChildren = dropLastElement(lastChildren);
            } else {
                getLast(inputs).explain(this);
            }
        }
    }

    private static <T> List<T> dropLastElement(List<T> originList) {
        if (originList == null || originList.isEmpty()) {
            throw new RuntimeException("dropLastElement cant support null or emtpy List");
        }
        return originList.subList(0, originList.size() - 1);
    }

    private static <T> T getLast(List<T> list) {
        if (list == null || list.isEmpty()) {
            throw new RuntimeException("getLast cant support null or emtpy List");
        }
        return list.get(list.size() - 1);
    }
}
