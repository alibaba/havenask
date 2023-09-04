package com.taobao.search.iquan.boot.common.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.springframework.stereotype.Component;

import java.util.List;

@Mapper
@Component
public interface IquanCatalogMapper {

    // ****************************************
    // Select Sqls
    // ****************************************
    @SelectProvider(type = CatalogSelectProvider.class, method = "selectAllCatalogNames")
    List<String> selectAllCatalogNames(@Param("tableName") String dbTableName);

    @SelectProvider(type = CatalogSelectProvider.class, method = "selectAllDatabaseNamesInCatalog")
    List<String> selectAllDatabaseNamesInCatalog(@Param("tableName") String dbTableName,
                                                 @Param("catalogName") String catalogName);

    @SelectProvider(type = CatalogSelectProvider.class, method = "selectAllTableNamesInDatabase")
    List<String> selectAllTableNamesInDatabase(@Param("tableName") String dbTableName,
                                               @Param("catalogName") String catalogName,
                                               @Param("databaseName") String databaseName);

    @SelectProvider(type = CatalogSelectProvider.class, method = "selectAllFunctionNamesInDatabase")
    List<String> selectAllFunctionNamesInDatabase(@Param("tableName") String dbTableName,
                                                  @Param("catalogName") String catalogName,
                                                  @Param("databaseName") String databaseName);

    class CatalogSelectProvider {
        public String selectAllCatalogNames(@Param("tableName") String dbTableName) {
            String sql = "SELECT DISTINCT(catalog_name) FROM " + dbTableName;
            return sql;
        }

        public String selectAllDatabaseNamesInCatalog(@Param("tableName") String dbTableName,
                                                      @Param("catalogName") String catalogName) {
            String sql = "SELECT DISTINCT(database_name) FROM " + dbTableName
                    + " WHERE catalog_name = #{catalogName}";
            return sql;
        }

        public String selectAllTableNamesInDatabase(@Param("tableName") String dbTableName,
                                                    @Param("catalogName") String catalogName,
                                                    @Param("databaseName") String databaseName) {
            String sql = "SELECT DISTINCT(table_name) FROM " + dbTableName
                    + " WHERE catalog_name = #{catalogName}"
                    + " AND database_name = #{databaseName}";
            return sql;
        }

        public String selectAllFunctionNamesInDatabase(@Param("tableName") String dbTableName,
                                                       @Param("catalogName") String catalogName,
                                                       @Param("databaseName") String databaseName) {
            String sql = "SELECT DISTINCT(function_name) FROM " + dbTableName
                    + " WHERE catalog_name = #{catalogName}"
                    + " AND database_name = #{databaseName}";
            return sql;
        }
    }
}
