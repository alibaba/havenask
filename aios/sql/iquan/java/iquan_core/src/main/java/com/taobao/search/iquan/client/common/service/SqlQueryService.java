package com.taobao.search.iquan.client.common.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlQueryService {
    private static final Logger logger = LoggerFactory.getLogger(SqlQueryService.class);

    public SqlQueryService() {
    }

    @SuppressWarnings("unchecked")
    public static SqlResponse sqlQuery(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        assert sqlTranslator != null;

        SqlResponse response = new SqlResponse();

        if (!reqMap.containsKey(ConstantDefine.SQLS)
                || !reqMap.containsKey(ConstantDefine.DEFAULT_CATALOG_NAME)
                || !reqMap.containsKey(ConstantDefine.DEFAULT_DATABASE_NAME)) {
            ErrorUtils.setIquanErrorCode(
                    IquanErrorCode.IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY,
                    String.format("%s or %s or %s",
                            ConstantDefine.SQLS,
                            ConstantDefine.DEFAULT_CATALOG_NAME,
                            ConstantDefine.DEFAULT_DATABASE_NAME),
                    response);
            return response;
        }

        List<String> sqls;
        String defaultCatalogName;
        String defaultDbName;
        try {
            sqls = (List<String>) reqMap.get(ConstantDefine.SQLS);
            defaultCatalogName = (String) reqMap.get(ConstantDefine.DEFAULT_CATALOG_NAME);
            defaultDbName = (String) reqMap.get(ConstantDefine.DEFAULT_DATABASE_NAME);
        } catch (Exception ex) {
            ErrorUtils.setIquanErrorCode(
                    IquanErrorCode.IQUAN_EC_BOOT_COMMON_INVALID_KEY,
                    String.format("%s or %s or %s",
                            ConstantDefine.SQLS,
                            ConstantDefine.DEFAULT_CATALOG_NAME,
                            ConstantDefine.DEFAULT_DATABASE_NAME),
                    response);
            return response;
        }

        if (sqls.isEmpty() || defaultCatalogName.isEmpty() || defaultDbName.isEmpty()) {
            ErrorUtils.setIquanErrorCode(
                    IquanErrorCode.IQUAN_EC_BOOT_COMMON_INVALID_KEY,
                    String.format("%s or %s or %s",
                            ConstantDefine.SQLS,
                            ConstantDefine.DEFAULT_CATALOG_NAME,
                            ConstantDefine.DEFAULT_DATABASE_NAME),
                    response);
            return response;
        }

        try {
            IquanConfigManager configManager = new IquanConfigManager();
            Map<String, Object> sqlParams = IquanRelOptUtils.<Map<String, Object>>getValueFromMap(reqMap, ConstantDefine.SQL_PARAMS, new HashMap<>());
            SqlConfigOptions.addToConfiguration(sqlParams, configManager);

            List<List<Object>> dynamicParams = IquanRelOptUtils.<List<List<Object>>>getValueFromMap(reqMap, ConstantDefine.DYNAMIC_PARAMS, new ArrayList<>());
            if (!dynamicParams.isEmpty() && dynamicParams.size() != sqls.size()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_BOOT_COMMON_INVALID_KEY,
                        ConstantDefine.DYNAMIC_PARAMS);
            }

            Object result;
            Map<String, Object> debugInfos = new HashMap<>();

            result = sqlTranslator.newSqlQueryable()
                    .select(sqls, dynamicParams, defaultCatalogName, defaultDbName, configManager, debugInfos);

            if (!debugInfos.isEmpty()) {
                logger.info("========");
                logger.info("sql: " + sqls.get(0));
                logger.info(String.format("default catalog name: %s, default db name: %s", defaultCatalogName, defaultDbName));
                logger.info("sql params: " + IquanRelOptUtils.toPrettyJson(sqlParams));
                logger.info("dynamic params: " + IquanRelOptUtils.toPrettyJson(dynamicParams));
                logger.info("debug infos: " + IquanRelOptUtils.toPrettyJson(debugInfos));
                logger.info("========");
                response.setDebugInfos(debugInfos);
            }
            response.setResult(result);
        } catch (SqlQueryException e) {
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            ErrorUtils.setException(e, response);
            logger.error("fail sqls: " + String.join(ConstantDefine.LIST_SEPERATE, sqls));
        }
        return response;
    }
}
