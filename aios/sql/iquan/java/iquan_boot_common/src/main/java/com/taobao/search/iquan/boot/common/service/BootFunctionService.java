package com.taobao.search.iquan.boot.common.service;

import com.taobao.search.iquan.boot.common.mapper.IquanFunctionMapper;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.service.FunctionService;
import com.taobao.search.iquan.boot.common.utils.DateUtils;
import com.taobao.search.iquan.core.api.SqlTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class BootFunctionService {
    private static final Logger logger = LoggerFactory.getLogger(BootFunctionService.class);

    @Autowired
    private IquanFunctionMapper mapper;

    public BootFunctionService() {
    }

    // ****************************************
    // Select Service For DB
    // ****************************************
    public List<IquanFunctionModel> selectAll(String dbTableName) {
        return mapper.selectAll(dbTableName);
    }

    public List<IquanFunctionModel> selectAllValidRecords(String dbTableName) {
        return mapper.selectAllValidRecords(dbTableName);
    }

    public List<IquanFunctionModel> selectAllRecordsWithTimeRange(String dbTableName, Date startTime, Date endTime) {
        return mapper.selectAllRecordsWithTimeRange(dbTableName,
                DateUtils.dateFormat(startTime),
                DateUtils.dateFormat(endTime));
    }

    public List<IquanFunctionModel> selectFunctionRecords(String dbTableName, IquanFunctionModel model) {
        return mapper.selectFunctionRecords(dbTableName, model);
    }

    public IquanFunctionModel selectFunctionMaxVersionRecord(String dbTableName, IquanFunctionModel model) {
        return mapper.selectFunctionMaxVersionRecord(dbTableName, model);
    }

    public long selectFunctionMaxVersion(String dbTableName, IquanFunctionModel model) {
        return mapper.selectFunctionMaxVersion(dbTableName, model);
    }

    public List<IquanFunctionModel> selectAllFunctionMaxVersionWithTimeRange(String dbTableName, Date startTime, Date endTime) {
        return mapper.selectAllFunctionMaxVersionWithTimeRange(dbTableName,
                DateUtils.dateFormat(startTime),
                DateUtils.dateFormat(endTime));
    }

    public List<IquanFunctionModel> selectAllFunctionMaxVersionRecordsWithTimeRange(String dbTableName, Date startTime, Date endTime) {
        return mapper.selectAllFunctionMaxVersionRecordsWithTimeRange(dbTableName,
                DateUtils.dateFormat(startTime),
                DateUtils.dateFormat(endTime));
    }


    // ****************************************
    // Insert Service For DB
    // ****************************************
    public int insertRecord(String dbTableName, IquanFunctionModel model) {
        return mapper.insertRecord(dbTableName, model);
    }


    // ****************************************
    // Delete Service For DB
    // ****************************************
    public int physicalDeleteAll(String dbTableName) {
        return mapper.physicalDeleteAll(dbTableName);
    }

    public int physicalDeleteAllInvalidRecords(String dbTableName) {
        return mapper.physicalDeleteAllInvalidRecords(dbTableName);
    }

    public int logicalDeleteValidRecords(String dbTableName, IquanFunctionModel model, List<Long> ids) {
        return mapper.logicalDeleteValidRecords(dbTableName, model, ids);
    }

    public int physicalDeleteInvalidRecords(String dbTableName, IquanFunctionModel model, List<Long> ids) {
        return mapper.physicalDeleteInvalidRecords(dbTableName, model, ids);
    }


    // ****************************************
    // Service For Catalog
    // ****************************************
    public SqlResponse updateFunctions(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return FunctionService.updateFunctions(sqlTranslator, reqMap);
    }

    public SqlResponse dropFunctions(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
        return FunctionService.dropFunctions(sqlTranslator, reqMap);
    }
}
