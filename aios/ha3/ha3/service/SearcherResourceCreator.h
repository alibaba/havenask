#ifndef ISEARCH_SEARCHERRESOURCECREATOR_H
#define ISEARCH_SEARCHERRESOURCECREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/common/ReturnInfo.h>
#include <suez/turing/expression/plugin/SorterModuleFactory.h>
#include <suez/turing/common/CavaJitWrapper.h>
#include <suez/turing/search/Biz.h>

BEGIN_HA3_NAMESPACE(service);

class SearcherResourceCreator
{
public:
    SearcherResourceCreator(const config::ConfigAdapterPtr &configAdapterPtr,
                            kmonitor::MetricsReporter *metricsReporter,
                            suez::turing::Biz *biz);
    ~SearcherResourceCreator();

public:
    common::ReturnInfo createSearcherResource(
            const std::string &clusterName,
            const proto::Range &hashIdRange,
            FullIndexVersion version,
            uint32_t partCount,
            const search::IndexPartitionWrapperPtr &oldIndexPartitionWrapperPtr,
                SearcherResourcePtr &searcherResourcePtr);

private:
    bool createRankConfigMgr(rank::RankProfileManagerPtr &rankProfileMgrPtr,
                             const suez::turing::TableInfoPtr &tableInfo,
                             const std::string &clusterName);

    config::HitSummarySchemaPtr createHitSummarySchema(const suez::turing::TableInfoPtr &tableInfoPtr);

    search::OptimizerChainManagerPtr createOptimizerChainManager(const std::string &clusterName);

    bool createSummaryConfigMgr(summary::SummaryProfileManagerPtr &summaryProfileMgrPtr,
                                config::HitSummarySchemaPtr &hitSummarySchemaPtr,
                                const std::string &clusterName,
                                const suez::turing::TableInfoPtr &tableInfoPtr);

    bool createSearcherCache(const std::string &clusterName,
                             search::SearcherCachePtr &searcherCachePtr);

    static bool validateAttributeFieldsForSummaryProfile(
            const config::SummaryProfileConfig &summaryProfileConfig,
            const suez::turing::TableInfoPtr &tableInfoPtr);

    friend class SearcherResourceCreatorTest;
public:
    // for test.
    void setResourceReader(const config::ResourceReaderPtr &resourceReader) {
        _resourceReaderPtr = resourceReader;
    }
private:
    config::ConfigAdapterPtr _configAdapterPtr;
    kmonitor::MetricsReporter *_metricsReporter;
    suez::turing::Biz *_biz;
    config::ResourceReaderPtr _resourceReaderPtr;
    std::string _dataPath;
    std::string _pluginDir;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherResourceCreator);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_SEARCHERRESOURCECREATOR_H
