#ifndef ISEARCH_DEFAULTSQLBIZ_H
#define ISEARCH_DEFAULTSQLBIZ_H

#include <ha3/config/ConfigAdapter.h>
#include <ha3/config/HitSummarySchema.h>
#include <ha3/config/ClusterTableInfoManager.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <ha3/sql/ops/tvf/TvfFuncManager.h>
#include <ha3/sql/ops/tvf/TvfSummaryResource.h>
#include <suez/turing/search/Biz.h>
#include <suez/search/BizMeta.h>
#include <navi/engine/Navi.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/summary/SummaryProfileManagerCreator.h>

BEGIN_HA3_NAMESPACE(turing);

class DefaultSqlBiz: public suez::turing::Biz
{
public:
    DefaultSqlBiz();
    ~DefaultSqlBiz();
private:
    DefaultSqlBiz(const DefaultSqlBiz &);
    DefaultSqlBiz& operator=(const DefaultSqlBiz &);
public:
    tensorflow::Status init(const std::string &bizName,
                            const suez::BizMeta &bizMeta,
                            const suez::turing::ProcessResource &processResource) override;
protected:
    tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override;
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status preloadBiz() override;
    std::string getBizInfoFile() override;
    tensorflow::Status loadUserPlugins() override;
private:
    std::string convertFunctionConf();
    bool initSqlAggFuncManager(const HA3_NS(config)::SqlAggPluginConfig &sqlAggPluginConfig);
    bool initSqlTvfFuncManager(const HA3_NS(config)::SqlTvfPluginConfig &sqlTvfPluginConfig);
    static bool getRange(uint32_t partCount, uint32_t partId, proto::Range &range);
    HA3_NS(sql)::TvfSummaryResource *perpareTvfSummaryResource();
    bool createSummaryConfigMgr(
            HA3_NS(summary)::SummaryProfileManagerPtr &summaryProfileMgrPtr,
            HA3_NS(config)::HitSummarySchemaPtr &hitSummarySchemaPtr,
            const std::string& clusterName,
            const suez::turing::TableInfoPtr &tableInfoPtr);
    std::string getConfigZoneBizName(const std::string &zoneName);
private:
    config::ConfigAdapterPtr _configAdapter;
    config::SqlConfigPtr _sqlConfigPtr;
    navi::NaviPtr _naviPtr;
    HA3_NS(sql)::SqlBizResourcePtr _sqlBizResourcePtr;
    HA3_NS(sql)::AggFuncManagerPtr _aggFuncManager;
    HA3_NS(sql)::TvfFuncManagerPtr _tvfFuncManager;
    config::ResourceReaderPtr _resourceReaderPtr;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultSqlBiz> DefaultSqlBizPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERBIZ_H
