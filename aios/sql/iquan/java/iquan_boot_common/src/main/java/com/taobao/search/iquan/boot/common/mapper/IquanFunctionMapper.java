package com.taobao.search.iquan.boot.common.mapper;

import com.taobao.search.iquan.boot.common.mapper.providers.delete.FunctionDeleteProvider;
import com.taobao.search.iquan.boot.common.mapper.providers.insert.FunctionInsertProvider;
import com.taobao.search.iquan.boot.common.mapper.providers.select.FunctionSelectProvider;
import com.taobao.search.iquan.client.common.model.IquanFunctionModel;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;

import java.util.List;

@Mapper
@Component
public interface IquanFunctionMapper {

    // ****************************************
    // Select Sqls
    // ****************************************
    @SelectProvider(type = FunctionSelectProvider.class, method = "selectAll")
    List<IquanFunctionModel> selectAll(@Param("tableName") String dbTableName);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectAllValidRecords")
    List<IquanFunctionModel> selectAllValidRecords(@Param("tableName") String dbTableName);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectAllRecordsWithTimeRange")
    List<IquanFunctionModel> selectAllRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                           @Param("start_time") String startTime,
                                                           @Param("end_time") String endTime);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectFunctionRecords")
    List<IquanFunctionModel> selectFunctionRecords(@Param("tableName") String dbTableName,
                                                   @Param("model") IquanFunctionModel model);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectFunctionMaxVersionRecord")
    IquanFunctionModel selectFunctionMaxVersionRecord(@Param("tableName") String dbTableName,
                                                      @Param("model") IquanFunctionModel model);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectFunctionMaxVersion")
    long selectFunctionMaxVersion(@Param("tableName") String dbTableName,
                                  @Param("model") IquanFunctionModel model);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectAllFunctionMaxVersionWithTimeRange")
    List<IquanFunctionModel> selectAllFunctionMaxVersionWithTimeRange(@Param("tableName") String dbTableName,
                                                                      @Param("start_time") String startTime,
                                                                      @Param("end_time") String endTime);

    @SelectProvider(type = FunctionSelectProvider.class, method = "selectAllFunctionMaxVersionRecordsWithTimeRange")
    List<IquanFunctionModel> selectAllFunctionMaxVersionRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                                             @Param("start_time") String startTime,
                                                                             @Param("end_time") String endTime);


    // ****************************************
    // Insert Sqls
    // ****************************************
    @InsertProvider(type = FunctionInsertProvider.class, method = "insertRecord")
    int insertRecord(@Param("tableName") String dbTableName,
                     @Param("model") IquanFunctionModel model);


    // ****************************************
    // Delete Sqls
    // ****************************************
    //@Options(statementType= StatementType.STATEMENT, useGeneratedKeys = true, flushCache = Options.FlushCachePolicy.TRUE)
    @DeleteProvider(type = FunctionDeleteProvider.class, method = "physicalDeleteAll")
    int physicalDeleteAll(@Param("tableName") String dbTableName);

    @DeleteProvider(type = FunctionDeleteProvider.class, method = "physicalDeleteAllInvalidRecords")
    int physicalDeleteAllInvalidRecords(@Param("tableName") String dbTableName);

    @DeleteProvider(type = FunctionDeleteProvider.class, method = "logicalDeleteValidRecords")
    int logicalDeleteValidRecords(@Param("tableName") String dbTableName,
                                  @Param("model") IquanFunctionModel model,
                                  @Param("ids") List<Long> ids);

    @DeleteProvider(type = FunctionDeleteProvider.class, method = "physicalDeleteInvalidRecords")
    int physicalDeleteInvalidRecords(@Param("tableName") String dbTableName,
                                     @Param("model") IquanFunctionModel model,
                                     @Param("ids") List<Long> ids);
}
