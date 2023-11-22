package com.taobao.search.iquan.boot.common.service;

import com.taobao.search.iquan.boot.common.mapper.IquanTableMapper;
import com.taobao.search.iquan.client.common.model.IquanTableModel;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.service.TableService;
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
public class BootTableService {
    private static final Logger logger = LoggerFactory.getLogger(BootTableService.class);

    @Autowired
    private IquanTableMapper mapper;

    public BootTableService() {
    }

    public String getDbTableName(String dbTableName) {
        return dbTableName;
    }


    // ****************************************
    // Select Service For DB
    // ****************************************
    public List<IquanTableModel> selectAll(String dbTableName) {
        return mapper.selectAll(dbTableName);
    }

    public List<IquanTableModel> selectAllValidRecords(String dbTableName) {
        return mapper.selectAllValidRecords(dbTableName);
    }

    public List<IquanTableModel> selectAllRecordsWithTimeRange(String dbTableName, Date startTime, Date endTime) {
        return mapper.selectAllRecordsWithTimeRange(dbTableName,
                DateUtils.dateFormat(startTime),
                DateUtils.dateFormat(endTime));
    }

    public List<IquanTableModel> selectTableRecords(String dbTableName, IquanTableModel model) {
        return mapper.selectTableRecords(dbTableName, model);
    }

    public IquanTableModel selectTableMaxVersionRecord(String dbTableName, IquanTableModel model) {
        return mapper.selectTableMaxVersionRecord(dbTableName, model);
    }

    public long selectTableMaxVersion(String dbTableName, IquanTableModel model) {
        return mapper.selectTableMaxVersion(dbTableName, model);
    }

    public List<IquanTableModel> selectAllTableMaxVersionWithTimeRange(String dbTableName, Date startTime, Date endTime) {
        return mapper.selectAllTableMaxVersionWithTimeRange(dbTableName,
                DateUtils.dateFormat(startTime),
                DateUtils.dateFormat(endTime));
    }

    public List<IquanTableModel> selectAllTableMaxVersionRecordsWithTimeRange(String dbTableName, Date startTime, Date endTime) {
        return mapper.selectAllTableMaxVersionRecordsWithTimeRange(dbTableName,
                DateUtils.dateFormat(startTime),
                DateUtils.dateFormat(endTime));
    }


    // ****************************************
    // Insert Service For DB
    // ****************************************
    public int insertRecord(String dbTableName, IquanTableModel model) {
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

    public int logicalDeleteValidRecords(String dbTableName, IquanTableModel model, List<Long> ids) {
        return mapper.logicalDeleteValidRecords(dbTableName, model, ids);
    }

    public int physicalDeleteInvalidRecords(String dbTableName, IquanTableModel model, List<Long> ids) {
        return mapper.physicalDeleteInvalidRecords(dbTableName, model, ids);
    }


    // ****************************************
    // Service For Catalog
    // ****************************************
//    public SqlResponse updateTables(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
//        return TableService.registerTables(sqlTranslator, reqMap);
//    }

//    public SqlResponse dropTables(SqlTranslator sqlTranslator, Map<String, Object> reqMap) {
//        return TableService.dropTables(sqlTranslator, reqMap);
//    }
}
