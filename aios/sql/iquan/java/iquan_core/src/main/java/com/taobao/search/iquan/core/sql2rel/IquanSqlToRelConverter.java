package com.taobao.search.iquan.core.sql2rel;

import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.ops.logical.CTEConsumer;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IquanSqlToRelConverter extends SqlToRelConverter {

    private final Map<SqlIdentifier, CTEProducer> cteNodes = new HashMap<>();

    public IquanSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config)
    {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    public RelRoot convertWith(SqlWith with, boolean top) {
        for (int i = 0; i < with.withList.size(); ++i) {
            SqlWithItem itemNode = (SqlWithItem) with.withList.get(i);
            String name = itemNode.name.toString();
            RelNode relNode = convertQuery(itemNode.query, false, false).rel;
            List<RelHint> hints = Collections.emptyList();
            if (relNode instanceof Hintable) {
                hints = IquanHintOptUtils.getHintsByCategory(
                        ((Hintable) relNode).getHints(), IquanHintCategory.CAT_CTE_ATTR);
            }
            CTEProducer producer = new CTEProducer(
                    cluster, relNode.getTraitSet(), relNode, name, i, hints);
            cteNodes.put(itemNode.name, producer);
        }
        return convertQuery(with.body, false, top);
    }

    @Override
    protected void convertFrom(
            Blackboard bb,
            SqlNode from)
    {
        if (from instanceof SqlWithItem) {
            SqlWithItem itemNode = (SqlWithItem) from;
            CTEProducer producer = cteNodes.get(itemNode.name);
            assert producer != null : String.format("cannot find cte [%s]", itemNode.name.toString());
            CTEConsumer consumer = new CTEConsumer(cluster, producer.getTraitSet(), producer);
            bb.setRoot(consumer, true);
        }
        else {
            super.convertFrom(bb, from);
        }
    }
}
