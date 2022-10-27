#ifndef ISEARCH_TURING_QRSSQLBIZ_H
#define ISEARCH_TURING_QRSSQLBIZ_H

#include <suez/turing/search/Biz.h>
#include <suez/search/BizMeta.h>
#include <ha3/monitor/QrsBizMetrics.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/service/QrsSearchConfig.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <ha3/sql/ops/tvf/TvfFuncManager.h>
#include <autil/mem_pool/Pool.h>
#include <navi/engine/Navi.h>
#include <iquan_jni/api/Iquan.h>

BEGIN_HA3_NAMESPACE(turing);

class QrsSqlBiz: public suez::turing::Biz
{
public:
    QrsSqlBiz();
    ~QrsSqlBiz();
private:
    QrsSqlBiz(const QrsSqlBiz &);
    QrsSqlBiz& operator=(const QrsSqlBiz &);
public:
    tensorflow::Status init(const std::string &bizName,
                            const suez::BizMeta &bizMeta,
                            const suez::turing::ProcessResource &processResource) override;
    monitor::Ha3BizMetricsPtr getBizMetrics() const;
    sql::SqlBizResourcePtr getSqlBizResource();
    sql::SqlQueryResourcePtr createSqlQueryResource(const kmonitor::MetricsTags &tags,
            const std::string &prefix, autil::mem_pool::Pool *pool);
    config::SqlConfigPtr getSqlConfig() { return _sqlConfigPtr; }
    navi::NaviPtr getNavi() const { return _naviPtr; }
    iquan::Iquan *getSqlClient() { return _sqlClientPtr.get(); }
    std::string getDefaultDatabaseName() { return _defaultDatabaseName; }
    std::string getDefaultCatalogName() { return _defaultCatalogName; }
    iquan::Status dumpCatalog(std::string &result) {
        return _sqlClientPtr->dumpCatalog(result);
    }

protected:
    tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override;
    tensorflow::QueryResourcePtr createQueryResource() override;
    bool updateFlowConfig(const std::string &zoneBizName) override;
    std::string getBizInfoFile() override;
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status preloadBiz() override;
    tensorflow::Status doCreateSession(suez::turing::TfSession &tfsession) override {
        return tensorflow::Status::OK();
    }
    tensorflow::Status loadUserPlugins() override;

private:
    tensorflow::Status initSqlClient();
    static std::string getSearchBizInfoFile(const std::string &bizName);
    bool isDefaultBiz(const std::string &bizName);
    std::string getSearchDefaultBizJson(const std::string &bizName,
            const std::string &dbName);
    tensorflow::Status getDependTables(const std::string &bizName,
            std::vector<std::string> &dependTables, std::string &dbName);
    tensorflow::Status fillHa3TableModels(
            const std::vector<std::string> &searcherBizNames,
            std::vector<iquan::Ha3TableModel> &tableInfos);
    tensorflow::Status fillTableModels(
            const std::vector<std::string> &searcherBizNames,
            iquan::TableModels &tableInfos);
    tensorflow::Status readClusterFile(const std::string &tableName,
            iquan::Ha3TableModel &tableInfo);
    tensorflow::Status fillFunctionModels(const std::vector<std::string> &searcherBizNames,
            iquan::FunctionModels &functionModels,
            iquan::TvfModels &tvfModels);
    bool initSqlAggFuncManager(const HA3_NS(config)::SqlAggPluginConfig &sqlAggPluginConfig);
    bool initSqlTvfFuncManager(const HA3_NS(config)::SqlTvfPluginConfig &sqlTvfPluginConfig);

    template <typename IquanModels>
    void addUserFunctionModels(const IquanModels &inModels,
                               const std::vector<std::string> &specialCatalogs,
                               const std::string &dbName,
                               IquanModels &functionModels);

private:
    config::ConfigAdapterPtr _configAdapter;
    monitor::Ha3BizMetricsPtr _bizMetrics;
    HA3_NS(sql)::SqlBizResourcePtr _sqlBizResource;
    config::SqlConfigPtr _sqlConfigPtr;
    navi::NaviPtr _naviPtr;
    std::unique_ptr<iquan::Iquan> _sqlClientPtr;
    std::string _defaultCatalogName;
    std::string _defaultDatabaseName;
    int64_t _tableVersion;
    int64_t _functionVersion;
    HA3_NS(sql)::AggFuncManagerPtr _aggFuncManager;
    HA3_NS(sql)::TvfFuncManagerPtr _tvfFuncManager;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<QrsSqlBiz> QrsSqlBizPtr;


template <typename IquanModels>
void QrsSqlBiz::addUserFunctionModels(const IquanModels &inModels,
                                      const std::vector<std::string> &specialCatalogs,
                                      const std::string &dbName,
                                      IquanModels &outModels)
{

    for (auto model : inModels.functions) {
        if (!model.catalogName.empty()) {
            model.databaseName = dbName;
            model.functionVersion = _functionVersion;
            outModels.functions.emplace_back(model);
        } else if (specialCatalogs.empty()){
            model.catalogName = SQL_DEFAULT_CATALOG_NAME;
            model.databaseName = dbName;
            model.functionVersion = _functionVersion;
            outModels.functions.emplace_back(model);
        } else {
            HA3_LOG(INFO, "use special catalog name: [%s]",
                    autil::StringUtil::toString(specialCatalogs).c_str());
            for (auto const &catalogName : specialCatalogs) {
                model.catalogName = catalogName;
                model.databaseName = dbName;
                model.functionVersion = _functionVersion;
                outModels.functions.emplace_back(model);
            }
        }
    }
}


END_HA3_NAMESPACE(turing);

#endif //ISEARCH_TURING_QRSSQLBIZ_H
