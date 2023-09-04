package com.taobao.search.iquan.core.rel.visitor.rexshuttle;

import com.taobao.search.iquan.client.common.json.function.JsonTvfFunction;
import com.taobao.search.iquan.client.common.json.function.JsonUdxfFunction;
import com.taobao.search.iquan.core.api.schema.MatchType;
import com.taobao.search.iquan.core.api.schema.TvfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.catalog.function.internal.ScalarFunction;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RexMatchTypeShuttle extends RexShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RexMatchTypeShuttle.class);
    private final Set<String> matchTypes = new HashSet<>(4);

    public RexMatchTypeShuttle() {
    }

    public List<String> getMathTypes() {
        return new ArrayList<>(matchTypes);
    }

    @Override
    public RexNode visitCall(final RexCall call) {
        RexCall newCall = (RexCall) super.visitCall(call);

        SqlOperator sqlOperator = newCall.getOperator();
        Map<String, Object> properties = null;
        if (sqlOperator instanceof ScalarFunction) {
            UdxfFunction udxfFunction = ((ScalarFunction) sqlOperator).getUdxfFunction();
            JsonUdxfFunction jsonUdxfFunction = udxfFunction.getJsonUdxfFunction();
            properties = jsonUdxfFunction.getProperties();
        } else if (sqlOperator instanceof TableValueFunction) {
            TvfFunction tvfFunction = ((TableValueFunction) sqlOperator).getTvfFunction();
            JsonTvfFunction jsonTvfFunction = tvfFunction.getJsonTvfFunction();
            properties = jsonTvfFunction.getProperties();
        }

        if (properties == null || !properties.containsKey(ConstantDefine.MATCH_TYPE)) {
            return newCall;
        }

        String strMatchTypes = properties.get(ConstantDefine.MATCH_TYPE).toString();
        String[] matchTypeList = strMatchTypes.split(ConstantDefine.VERTICAL_LINE_REGEX);
        for (String type : matchTypeList) {
            MatchType matchType = MatchType.from(type);
            if (!matchType.isValid()) {
                logger.warn("not support match type {} for function {}, skip...", type, sqlOperator.getName());
                continue;
            }
            matchTypes.add(matchType.getName());
        }
        return newCall;
    }
}
