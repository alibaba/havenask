package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.CharStreams;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecisionRuleTable {
    private static final Logger logger = LoggerFactory.getLogger(DecisionRuleTable.class);

    private static final Map<Pattern, PhysicalJoinFactory> decisionMap = new HashMap<>();

    static {
        loadRuleTable(decisionMap, ConstantDefine.JOIN_DECISION_MAP_FILE);
    }

    public static PhysicalJoinFactory search(Pattern pattern) {
        return decisionMap.get(pattern);
    }

    private static void loadRuleTable(Map<Pattern, PhysicalJoinFactory> decisionMap, String ruleFile) {
        JsonRule[] jsonRules = readRuleFile(ruleFile);
        if (jsonRules == null) {
            logger.error("read rule file {} fail.", ruleFile);
            return;
        }

        for (JsonRule jsonRule : jsonRules) {
            JsonPattern jsonPattern = jsonRule.pattern;
            JsonResult jsonResult = jsonRule.result;

            Pattern pattern = new Pattern(jsonPattern);
            PhysicalJoinFactory factory = PhysicalJoinFactory.createInstance(jsonResult);
            decisionMap.put(pattern, factory);
        }
    }

    private static JsonRule[] readRuleFile(String ruleFile) {
        if (ruleFile == null || ruleFile.isEmpty()) {
            logger.error("ruleFile == null || ruleFile.isEmpty()");
            return null;
        }

        try {
            InputStream inputStream = DecisionRuleTable.class.getClassLoader().getResourceAsStream(ruleFile);
            if (inputStream == null) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, String.format("read rule file %s fail", ruleFile));
            }

            String content = CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            JsonRule[] ruleTable = IquanRelOptUtils.fromJson(content, JsonRule[].class);
            boolean isValid = Arrays.stream(ruleTable).allMatch(JsonRule::isValid);
            if (!isValid) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, "some rules is not valid");
            }
            return ruleTable;
        } catch (Exception ex) {
            logger.error("caught exception when read join rule file:", ex);
            throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, "caught exception when read join rule file:", ex);
        }
    }
}
