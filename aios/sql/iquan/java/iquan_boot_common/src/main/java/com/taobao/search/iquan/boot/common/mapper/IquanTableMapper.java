package com.taobao.search.iquan.boot.common.mapper;

import com.taobao.search.iquan.boot.common.mapper.providers.delete.TableDeleteProvider;
import com.taobao.search.iquan.boot.common.mapper.providers.insert.TableInsertProvider;
import com.taobao.search.iquan.boot.common.mapper.providers.select.TableSelectProvider;
import com.taobao.search.iquan.client.common.model.IquanTableModel;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;

import java.util.List;

@Mapper
@Component
public interface IquanTableMapper {

    // ****************************************
    // Select Sqls
    // ****************************************
    @SelectProvider(type = TableSelectProvider.class, method = "selectAll")
    List<IquanTableModel> selectAll(@Param("tableName") String dbTableName);

    @SelectProvider(type = TableSelectProvider.class, method = "selectAllValidRecords")
    List<IquanTableModel> selectAllValidRecords(@Param("tableName") String dbTableName);

    @SelectProvider(type = TableSelectProvider.class, method = "selectAllRecordsWithTimeRange")
    List<IquanTableModel> selectAllRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                        @Param("start_time") String startTime,
                                                        @Param("end_time") String endTime);

    @SelectProvider(type = TableSelectProvider.class, method = "selectTableRecords")
    List<IquanTableModel> selectTableRecords(@Param("tableName") String dbTableName,
                                             @Param("model") IquanTableModel model);

    @SelectProvider(type = TableSelectProvider.class, method = "selectTableMaxVersionRecord")
    IquanTableModel selectTableMaxVersionRecord(@Param("tableName") String dbTableName,
                                                @Param("model") IquanTableModel model);

    @SelectProvider(type = TableSelectProvider.class, method = "selectTableMaxVersion")
    long selectTableMaxVersion(@Param("tableName") String dbTableName,
                               @Param("model") IquanTableModel model);

    @SelectProvider(type = TableSelectProvider.class, method = "selectAllTableMaxVersionWithTimeRange")
    List<IquanTableModel> selectAllTableMaxVersionWithTimeRange(@Param("tableName") String dbTableName,
                                                                @Param("start_time") String startTime,
                                                                @Param("end_time") String endTime);

    @SelectProvider(type = TableSelectProvider.class, method = "selectAllTableMaxVersionRecordsWithTimeRange")
    List<IquanTableModel> selectAllTableMaxVersionRecordsWithTimeRange(@Param("tableName") String dbTableName,
                                                                       @Param("start_time") String startTime,
                                                                       @Param("end_time") String endTime);


    // ****************************************
    // Insert Sqls
    // ****************************************
    @InsertProvider(type = TableInsertProvider.class, method = "insertRecord")
    int insertRecord(@Param("tableName") String dbTableName,
                     @Param("model") IquanTableModel model);


    // ****************************************
    // Delete Sqls
    // ****************************************
    //@Options(statementType= StatementType.STATEMENT, useGeneratedKeys = true, flushCache = Options.FlushCachePolicy.TRUE)
    @DeleteProvider(type = TableDeleteProvider.class, method = "physicalDeleteAll")
    int physicalDeleteAll(@Param("tableName") String dbTableName);

    @DeleteProvider(type = TableDeleteProvider.class, method = "physicalDeleteAllInvalidRecords")
    int physicalDeleteAllInvalidRecords(@Param("tableName") String dbTableName);

    @DeleteProvider(type = TableDeleteProvider.class, method = "logicalDeleteValidRecords")
    int logicalDeleteValidRecords(@Param("tableName") String dbTableName,
                                  @Param("model") IquanTableModel model,
                                  @Param("ids") List<Long> ids);

    @DeleteProvider(type = TableDeleteProvider.class, method = "physicalDeleteInvalidRecords")
    int physicalDeleteInvalidRecords(@Param("tableName") String dbTableName,
                                     @Param("model") IquanTableModel model,
                                     @Param("ids") List<Long> ids);
}
