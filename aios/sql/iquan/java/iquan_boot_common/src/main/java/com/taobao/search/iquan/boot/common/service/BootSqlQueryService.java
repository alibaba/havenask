package com.taobao.search.iquan.boot.common.service;

import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.service.SqlQueryService;
import com.taobao.search.iquan.core.api.SqlTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class BootSqlQueryService {
    private static final Logger logger = LoggerFactory.getLogger(BootSqlQueryService.class);

    public BootSqlQueryService() {

    }

    public SqlResponse sqlQuery(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return SqlQueryService.sqlQuery(sqlTranslator, reqMap);
    }
}
