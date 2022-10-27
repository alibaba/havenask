#ifndef ISEARCH_REQUEST_H
#define ISEARCH_REQUEST_H

#include <string>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/Serialize.h>
#include <ha3/common/QueryClause.h>
#include <ha3/common/AggregateClause.h>
#include <ha3/common/ConfigClause.h>
#include <ha3/common/RankClause.h>
#include <ha3/common/SortClause.h>
#include <ha3/common/DistinctClause.h>
#include <ha3/common/DocIdClause.h>
#include <ha3/common/FilterClause.h>
#include <ha3/common/ClusterClause.h>
#include <ha3/common/PKFilterClause.h>
#include <ha3/common/HealthCheckClause.h>
#include <ha3/common/AttributeClause.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <ha3/common/FetchSummaryClause.h>
#include <ha3/common/KVPairClause.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3/common/SearcherCacheClause.h>
#include <ha3/common/OptimizerClause.h>
#include <ha3/common/RankSortClause.h>
#include <ha3/common/FinalSortClause.h>
#include <ha3/common/LevelClause.h>
#include <ha3/common/AnalyzerClause.h>
#include <autil/mem_pool/Pool.h>
#include <autil/DataBuffer.h>

BEGIN_HA3_NAMESPACE(common);

class Request
{
public:
    Request(autil::mem_pool::Pool *pool = NULL);
    ~Request();
private:
    Request(const Request &request);
    Request& operator = (const Request& request);
public:
    void setQueryClause(QueryClause *queryClause);
    QueryClause *getQueryClause() const;
    QueryClause *stealQueryClause();
    QueryClause *cloneQueryClause();

    void setFilterClause(FilterClause *filterClause);
    FilterClause *getFilterClause() const;
    FilterClause *stealFilterClause();
    FilterClause *cloneFilterClause();

    void setPKFilterClause(PKFilterClause *pkFilterClause);
    PKFilterClause *getPKFilterClause() const;
    PKFilterClause *stealPKFilterClause();
    PKFilterClause *clonePKFilterClause();

    void setSortClause(SortClause *sortClause);
    SortClause *getSortClause() const;
    SortClause *stealSortClause();
    SortClause *cloneSortClause();

    void setDistinctClause(DistinctClause *distinctClause);
    DistinctClause *getDistinctClause() const;
    DistinctClause *stealDistinctClause();
    DistinctClause *cloneDistinctClause();

    void setAggregateClause(AggregateClause *aggClause);
    AggregateClause *getAggregateClause() const;
    AggregateClause *stealAggregateClause();
    AggregateClause *cloneAggregateClause();

    void setRankClause(RankClause *rankClause);
    RankClause *getRankClause() const;
    RankClause *stealRankClause();
    RankClause *cloneRankClause();

    void setConfigClause(ConfigClause *configClause);
    ConfigClause *getConfigClause() const;
    ConfigClause *stealConfigClause();
    ConfigClause *cloneConfigClause();

    void setDocIdClause(DocIdClause *docIdClause);
    DocIdClause *getDocIdClause() const;
    DocIdClause *stealDocIdClause();
    DocIdClause *cloneDocIdClause();

    void setClusterClause(ClusterClause *clusterClause);
    ClusterClause *getClusterClause() const;
    ClusterClause *stealClusterClause();
    ClusterClause *cloneClusterClause();

    void setHealthCheckClause(HealthCheckClause *healthCheckClause);
    HealthCheckClause* getHealthCheckClause() const;
    HealthCheckClause* stealHealthCheckClause();
    HealthCheckClause* cloneHealthCheckClause();

    void setAttributeClause(AttributeClause *attributeClause);
    AttributeClause* getAttributeClause() const;
    AttributeClause* stealAttributeClause();
    AttributeClause* cloneAttributeClause();

    void setVirtualAttributeClause(VirtualAttributeClause *virtualAttributeClause);
    VirtualAttributeClause* getVirtualAttributeClause() const;
    VirtualAttributeClause* stealVirtualAttributeClause();
    VirtualAttributeClause* cloneVirtualAttributeClause();

    void setFetchSummaryClause(FetchSummaryClause *fetchSummaryClause);
    FetchSummaryClause* getFetchSummaryClause() const;
    FetchSummaryClause* stealFetchSummaryClause();
    FetchSummaryClause* cloneFetchSummaryClause();

    void setKVPairClause(KVPairClause *kvPairClause);
    KVPairClause *getKVPairClause() const;
    KVPairClause *stealKVPairClause();

    void setQueryLayerClause(QueryLayerClause *queryLayerClause);
    QueryLayerClause *getQueryLayerClause() const;
    QueryLayerClause *stealQueryLayerClause();
    QueryLayerClause *cloneQueryLayerClause();

    void setSearcherCacheClause(SearcherCacheClause* cacheClause);
    SearcherCacheClause* getSearcherCacheClause() const;
    SearcherCacheClause* stealSearcherCacheClause();
    SearcherCacheClause* cloneSearcherCacheClause();

    void setOptimizerClause(OptimizerClause* cacheClause);
    OptimizerClause* getOptimizerClause() const;
    OptimizerClause* stealOptimizerClause();
    OptimizerClause* cloneOptimizerClause();

    void setRankSortClause(RankSortClause* rankSortClause);
    RankSortClause* getRankSortClause() const;
    RankSortClause* stealRankSortClause();
    RankSortClause* cloneRankSortClause();

    void setFinalSortClause(FinalSortClause* finalSortClause);
    FinalSortClause* getFinalSortClause() const;
    FinalSortClause* stealFinalSortClause();
    FinalSortClause* cloneFinalSortClause();

    void setLevelClause(LevelClause *levelClause);
    LevelClause* getLevelClause() const;
    LevelClause* stealLevelClause();
    LevelClause* cloneLevelClause();

    void setAnalyzerClause(AnalyzerClause *analyzerClause);
    AnalyzerClause* getAnalyzerClause() const;
    AnalyzerClause* stealAnalyzerClause();
    AnalyzerClause* cloneAnalyzerClause();

    void setAuxQueryClause(AuxQueryClause *auxQueryClause);
    AuxQueryClause *getAuxQueryClause() const;
    AuxQueryClause *stealAuxQueryClause();
    AuxQueryClause *cloneAuxQueryClause();

    void setAuxFilterClause(AuxFilterClause *auxFilterClause);
    AuxFilterClause *getAuxFilterClause() const;
    AuxFilterClause *stealAuxFilterClause();
    AuxFilterClause *cloneAuxFilterClause();

    void setRowKey(uint64_t key) const {
        _rowKey = key;
    }
    uint64_t getRowKey() const {
        return _rowKey;
    }

    Request* clone(autil::mem_pool::Pool *pool);

public:
    void setPartitionMode(const std::string& partitionMode);
    const std::string& getPartitionMode() const;

    void setOriginalString(const std::string& originalString);
    const std::string getOriginalString() const;

    HA3_SERIALIZE_DECLARE();
    void serializeWithoutAuxQuery(autil::DataBuffer &dataBuffer) const;

    void serializeToString(std::string &str) const;
    void deserializeFromString(const std::string &str,
                               autil::mem_pool::Pool *pool = NULL);

    const std::vector<std::string>& getUnknownClause() const;

    autil::mem_pool::Pool *getPool() const { return _pool; }
    void setPool(autil::mem_pool::Pool *pool) { _pool = pool; }
    void release();

    std::string toString() const;
    void setDegradeLevel(float level, uint32_t rankSize, uint32_t rerankSize);
    void getDegradeLevel(float &level, uint32_t &rankSize, uint32_t &rerankSize) const;
    void setUseGrpc(bool value) {
        _useGrpc = value;
    }
    bool useGrpc() const {
        return _useGrpc;
    }
private:
    void splitClause(const std::string& oriStr);
    bool setClauseString(const std::string& clauseName, const std::string& clauseString);

    void setQueryClauseString(const std::string &clauseString);
    void setFilterClauseString(const std::string &clauseString);
    void setPKFilterClauseString(const std::string &clauseString);
    void setSortClauseString(const std::string &clauseString);
    void setDistinctClauseString(const std::string &clauseString);
    void setAggregateClauseString(const std::string &clauseString);
    void setRankClauseString(const std::string &clauseString);
    void setConfigClauseString(const std::string &clauseString);
    void setClusterClauseString(const std::string &clauseString);
    void setHealthCheckClauseString(const std::string &clauseString);
    void setAttributeClauseString(const std::string &clauseString);
    void setVirtualAttributeClauseString(const std::string &clauseString);
    void setFetchSummaryClauseString(const std::string &clauseString);
    void setKVPairClauseString(const std::string &clauseString);
    void setQueryLayerClauseString(const std::string &clauseString);
    void setSearcherCacheClauseString(const std::string &clauseString);
    void setOptimizerClauseString(const std::string &clauseString);
    void setRankSortClauseString(const std::string &clauseString);
    void setFinalSortClauseString(const std::string &clauseString);
    void setLevelClauseString(const std::string &clauseString);
    void setAnalyzerClauseString(const std::string &clauseString);
    void setAuxQueryClauseString(const std::string &clauseString);
    void setAuxFilterClauseString(const std::string &clauseString);

private:
    ConfigClause *_configClause;
    QueryClause *_queryClause;
    FilterClause *_filterClause;
    PKFilterClause *_pkFilterClause;
    SortClause *_sortClause;
    DistinctClause* _distinctClause;
    AggregateClause *_aggregateClause;
    RankClause *_rankClause;
    DocIdClause *_docIdClause;
    ClusterClause *_clusterClause;
    HealthCheckClause *_healthCheckClause;
    AttributeClause *_attributeClause;
    VirtualAttributeClause *_virtualAttributeClause;
    FetchSummaryClause *_fetchSummaryClause;
    KVPairClause *_kvPairClause;
    QueryLayerClause *_queryLayerClause;
    SearcherCacheClause *_searcherCacheClause;
    OptimizerClause *_optimizerClause;
    RankSortClause *_rankSortClause;
    FinalSortClause *_finalSortClause;
    LevelClause *_levelClause;
    AnalyzerClause *_analyzerClause;
    AuxQueryClause *_auxQueryClause;
    AuxFilterClause *_auxFilterClause;

    std::string _partitionMode;
    std::vector<std::string> _unknownClause;
    // used in proxy
    std::string _curClusterName;
    mutable uint64_t _rowKey;

    autil::mem_pool::Pool *_pool;
    // searcher degrade
    float _degradeLevel;
    uint32_t _rankSize;
    uint32_t _rerankSize;

    bool _useGrpc;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Request);
typedef std::vector<RequestPtr> RequestPtrVector;
typedef std::map<std::string, std::string> ClauseStringMap;

END_HA3_NAMESPACE(common);

#endif //ISEARCH_REQUEST_H
