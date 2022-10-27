#ifndef ISEARCH_SEARCHERRESOURCE_H
#define ISEARCH_SEARCHERRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/partition/index_partition.h>
#include <indexlib/partition/partition_reader_snapshot.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/config/AggSamplerConfigInfo.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <ha3/search/OptimizerChainManager.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/service/ServiceDegrade.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>

BEGIN_HA3_NAMESPACE(search);
class SearcherCache;
HA3_TYPEDEF_PTR(SearcherCache);
END_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(service);

class SearcherResource
{
public:
    SearcherResource() {
        _fullIndexVersion = 0;
        _partCount = 0;
    }
    ~SearcherResource() {}
public:
    void setClusterName(const std::string &name) {
        _clusterName = name;
    }
    std::string getClusterName() const {
        return _clusterName;
    }
    void setTableInfo(const suez::turing::TableInfoPtr &tableInfoPtr) {
        _tableInfoPtr = tableInfoPtr;
    }
    suez::turing::TableInfoPtr getTableInfo() const {
        return _tableInfoPtr;
    }

    void setRankProfileManager(
            const rank::RankProfileManagerPtr &rankProfileMgrPtr)
    {
        _rankProfileMgrPtr = rankProfileMgrPtr;
    }
    rank::RankProfileManagerPtr getRankProfileManager() const {
        return _rankProfileMgrPtr;
    }

    void setFunctionCreator(const suez::turing::FunctionInterfaceCreatorPtr &funcCreatorPtr) {
        _funcCreatorPtr = funcCreatorPtr;
    }

    suez::turing::FunctionInterfaceCreatorPtr getFuncCreator() const{
        return _funcCreatorPtr;
    }

    void setHashIdRange(proto::Range hashIdRange) {
        _hashIdRange = hashIdRange;
    }
    proto::Range getHashIdRange() const {
        return _hashIdRange;
    }

    void setFullIndexVersion(FullIndexVersion fullIndexVersion) {
        _fullIndexVersion = fullIndexVersion;
    }
    FullIndexVersion getFullIndexVersion() const {
        return _fullIndexVersion;
    }

    void setPartCount(uint32_t partCount) {
        _partCount = partCount;
    }

    uint32_t getPartCount() const {
        return _partCount;
    }

    void setAggSamplerConfigInfo(const config::AggSamplerConfigInfo
                                 &aggSamplerConfigInfo)
    {
        _aggSamplerConfigInfo = aggSamplerConfigInfo;
    }

    const config::AggSamplerConfigInfo &getAggSamplerConfigInfo() const {
        return _aggSamplerConfigInfo;
    }

    void setSummaryProfileManager(const summary::SummaryProfileManagerPtr
                                  &summaryProfileManagerPtr)
    {
        _summaryProfileManagerPtr = summaryProfileManagerPtr;
    }

    summary::SummaryProfileManagerPtr getSummaryProfileManager() {
        return _summaryProfileManagerPtr;
    }

    void setHitSummarySchema(const config::HitSummarySchemaPtr &hitSummarySchemaPtr) {
        _hitSummarySchemaPtr = hitSummarySchemaPtr;
    }

    const config::HitSummarySchemaPtr &getHitSummarySchema() const {
        return _hitSummarySchemaPtr;
    }
    
    void setHitSummarySchemaPool(const config::HitSummarySchemaPoolPtr &hitSummarySchemaPool) {
        _hitSummarySchemaPool = hitSummarySchemaPool;
    }

    const config::HitSummarySchemaPoolPtr &getHitSummarySchemaPool() const {
        return _hitSummarySchemaPool;
    }

    void setOptimizerChainManager(const search::OptimizerChainManagerPtr &optimizerChainManagerPtr) {
        _optimizerChainManagerPtr = optimizerChainManagerPtr;
    }

    const search::OptimizerChainManagerPtr &getOptimizerChainManager() const {
        return _optimizerChainManagerPtr;
    }

    void setSearcherCache(search::SearcherCachePtr &searcherCachePtr) {
        _searcherCachePtr = searcherCachePtr;
    }

    search::SearcherCachePtr &getSearcherCache() {
        return _searcherCachePtr;
    }

    void setSorterManager(const suez::turing::SorterManagerPtr &sorterManagerPtr) {
        _sorterManagerPtr = sorterManagerPtr;
    }

    const suez::turing::SorterManagerPtr &getSorterManager() const {
        return _sorterManagerPtr;
    }

    void setClusterConfig(const config::ClusterConfigInfo& clusterConfig) {
        _clusterConfig = clusterConfig;
    }

    const config::ClusterConfigInfo &getClusterConfig() const {
        return _clusterConfig;
    }

    void setServiceDegrade(const ServiceDegradePtr &serviceDegradePtr) {
        _serviceDegradePtr = serviceDegradePtr;
    }

    const ServiceDegradePtr &getServiceDegrade() const {
        return _serviceDegradePtr;
    }

    void setIndexPartitionWrapper(
            const search::IndexPartitionWrapperPtr &indexPartitionWrapperPtr)
    {
        _indexPartitionWrapperPtr = indexPartitionWrapperPtr;
    }

    const search::IndexPartitionWrapperPtr& getIndexPartitionWrapper() const {
        return _indexPartitionWrapperPtr;
    }

    void setCavaPluginManager(const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr) {
        _cavaPluginManagerPtr = cavaPluginManagerPtr;
    }

    const suez::turing::CavaPluginManagerPtr& getCavaPluginManager()
    {
        return _cavaPluginManagerPtr;
    }

    search::IndexPartitionReaderWrapperPtr createIndexPartitionReaderWrapper(
            IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr indexSnapshot) const {
        assert(_indexPartitionWrapperPtr);
        return _indexPartitionWrapperPtr->createReaderWrapper(indexSnapshot);
    }

    search::IndexPartitionReaderWrapperPtr createPartialIndexPartitionReaderWrapper(
            IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr indexSnapshot) const {
        assert(_indexPartitionWrapperPtr);
        return _indexPartitionWrapperPtr->createPartialReaderWrapper(indexSnapshot);
    }

    proto::PartitionID getPartitionId() const {
        proto::PartitionID partId;
        partId.set_role(proto::ROLE_SEARCHER);
        partId.set_clustername(_clusterName);
        *(partId.mutable_range()) = _hashIdRange;
        partId.set_fullversion(_fullIndexVersion);
        return partId;
    }
private:
    suez::turing::TableInfoPtr _tableInfoPtr;
    rank::RankProfileManagerPtr _rankProfileMgrPtr;
    suez::turing::FunctionInterfaceCreatorPtr _funcCreatorPtr;
    summary::SummaryProfileManagerPtr _summaryProfileManagerPtr;
    config::HitSummarySchemaPtr _hitSummarySchemaPtr;
    config::HitSummarySchemaPoolPtr _hitSummarySchemaPool;
    search::OptimizerChainManagerPtr _optimizerChainManagerPtr;
    std::string _clusterName;
    FullIndexVersion _fullIndexVersion;
    uint32_t _partCount;
    config::AggSamplerConfigInfo _aggSamplerConfigInfo;
    proto::Range _hashIdRange;
    search::SearcherCachePtr _searcherCachePtr;
    config::ClusterConfigInfo _clusterConfig;
    suez::turing::SorterManagerPtr _sorterManagerPtr;
    ServiceDegradePtr _serviceDegradePtr;
    search::IndexPartitionWrapperPtr _indexPartitionWrapperPtr;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
private:
    HA3_LOG_DECLARE();
private:
    friend class SearcherResourceCreatorTest;
};

HA3_TYPEDEF_PTR(SearcherResource);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_SEARCHERRESOURCE_H
