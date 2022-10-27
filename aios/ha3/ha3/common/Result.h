#ifndef ISEARCH_RESULT_H
#define ISEARCH_RESULT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hits.h>
#include <ha3/common/AggregateResult.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/MultiErrorResult.h>
#include <ha3/common/PhaseOneSearchInfo.h>
#include <ha3/common/PhaseTwoSearchInfo.h>
#include <ha3/common/SortExprMeta.h>

BEGIN_HA3_NAMESPACE(common);
typedef std::map<std::string, std::string> Meta;
typedef std::map<std::string, Meta> MetaMap;
class Result;
HA3_TYPEDEF_PTR(Result);

class Result
{
public:
    typedef std::vector<std::pair<uint32_t, uint32_t> > PartitionRanges;
    typedef std::map<std::string, PartitionRanges> ClusterPartitionRanges;
public:
    Result(const ErrorCode errorCode = ERROR_NONE, const std::string &errorMsg = "");
    Result(Hits *hits);
    Result(MatchDocs *matchDocs);
    ~Result();
public:
    void setHits(Hits *hits);
    Hits *getHits() const;
    Hits *stealHits();
    uint32_t getTotalHits() const;
    void setClusterInfo(const std::string &clusterName, clusterid_t clusterId);

    // for plugin!!!
    void mergeByAppend(ResultPtr &resultPtr, bool doDedup = false);
    void mergeByDefaultSort(ResultPtr &resultPtr, bool doDedup = false);

    //used by merging interface in 'proxy' role of first-phase search
    void setMatchDocs(MatchDocs *matchDocs);
    MatchDocs *getMatchDocs() const;
    void clearMatchDocs();
    uint32_t getTotalMatchDocs() const;
    void setTotalMatchDocs(uint32_t totalMatchDocs);
    uint32_t getActualMatchDocs() const;
    void setActualMatchDocs(uint32_t actualMatchDocs);
    AggregateResults& getAggregateResults();
    uint32_t getAggregateResultCount() const;
    AggregateResultPtr getAggregateResult(uint32_t offset) const;
    void addAggregateResult(AggregateResultPtr aggregateResultPtr);
    void fillAggregateResults(const common::AggregateResultsPtr &aggResultsPtr);
    void addErrorResult(const ErrorResult &error) {
        _mErrorResult->addErrorResult(error);
    }
    void addErrorResult(const MultiErrorResultPtr& error){
        _mErrorResult->addErrorResult(error);
    }
    void resetErrorResult() {
        _mErrorResult->resetErrors();
    }
    void setErrorHostInfo(const std::string& partitionID,
                          const std::string& hostName = "");
    MultiErrorResultPtr &getMultiErrorResult() {
        return _mErrorResult;
    }
    const MultiErrorResultPtr &getMultiErrorResult() const {
        return _mErrorResult;
    }
    void setTotalTime(int64_t totalTime) {
        _totalTime = totalTime;
    }
    int64_t getTotalTime() {
        return _totalTime;
    }
    bool hasError() {
        return _mErrorResult->hasError();
    }
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool);

    void serializeToString(std::string &str, autil::mem_pool::Pool *pool = NULL) const;
    bool deserializeFromString(const char *data, size_t size, autil::mem_pool::Pool *pool = NULL);
    bool deserializeFromString(const std::string &str, autil::mem_pool::Pool *pool = NULL);

    void setTracer(TracerPtr tracer);
    Tracer *getTracer() const;
    TracerPtr getTracerPtr() const;

    void addCoveredRange(const std::string &clusterName, uint32_t from, uint32_t to) {
        _coveredRanges[clusterName].push_back(std::make_pair(from, to));
    }
    const ClusterPartitionRanges &getCoveredRanges() const {
        return _coveredRanges;
    }
    ClusterPartitionRanges &getCoveredRanges() {
        return _coveredRanges;
    }
    void addGlobalVariableMap(const AttributeItemMapPtr &m) {
        _globalVariables.push_back(m);
    }
    void addGlobalVariables(const std::vector<AttributeItemMapPtr> &vec) {
        _globalVariables.insert(_globalVariables.end(), vec.begin(), vec.end());
    }
    std::vector<matchdoc::MatchDoc> &getMatchDocsForFormat() {
        return _formatedDocs;
    }
    void addClusterName(const std::string &clusterName) {
        _clusterNames.push_back(clusterName);
    }
    const std::vector<std::string> &getClusterNames() const {
        return _clusterNames;
    }
    const std::vector<AttributeItemMapPtr> &getGlobalVarialbes() const {
        return _globalVariables;
    }
    template<typename T>
    std::vector<T *> getGlobalVarialbes(const std::string &name) const;
    const std::vector<SortExprMeta> &getSortExprMeta() const {
        return _sortExprMetaVec;
    }
    void setSortExprMeta(const std::vector<SortExprMeta> &sortExprMetaVec) {
        _sortExprMetaVec = sortExprMetaVec;
    }
    void setPhaseOneSearchInfo(PhaseOneSearchInfo *searchInfo);
    void setPhaseTwoSearchInfo(PhaseTwoSearchInfo *searchInfo);
    PhaseOneSearchInfo* getPhaseOneSearchInfo() const;
    PhaseTwoSearchInfo* getPhaseTwoSearchInfo() const;

    void mergePhaseOneSearchInfos(const std::vector<ResultPtr> &inputs);

    void setUseTruncateOptimizer(bool flag);
    bool useTruncateOptimizer() const;

    // for searcher cache
    void setSerializeLevel(uint8_t serializeLevel) {
        _serializeLevel = serializeLevel;
        if (_matchDocs) {
            _matchDocs->setSerializeLevel(serializeLevel);
        }
    }
    uint8_t getSerializeLevel() const {
        return _serializeLevel;
    }
    void setLackResult(bool lackResult) {
        _lackResult = lackResult;
    }
    bool lackResult() const {
        return _lackResult;
    }

    void setSrcCount(uint16_t srcCount) {
        _srcCount = srcCount;
    }

    uint16_t getSrcCount() const {
        return _srcCount;
    }

    /** these interfaces are for visit 'Meta', which is insert by 'QrsProcessor'. */
    const MetaMap &getMetaMap() const { return _metaMap; }
    void addMeta(const std::string &metaKey, const Meta &meta) {
        _metaMap[metaKey] = meta;
    }
    Meta &getMeta(const std::string &metaKey) {
        return _metaMap[metaKey];
    }


public:
    template<typename T>
    static std::vector<T *> getGlobalVarialbes(const std::string &name,
            const std::vector<AttributeItemMapPtr> &globalVariables);

private:
    void mergeAggregateResults(AggregateResults &aggregateResults);
    void clear();
    void init();

private:
    Hits *_hits;
    MatchDocs *_matchDocs;
    std::vector<matchdoc::MatchDoc> _formatedDocs;
    std::vector<std::string> _clusterNames;
    AggregateResults _aggResults;
    MultiErrorResultPtr _mErrorResult;
    TracerPtr _tracer;
    PhaseOneSearchInfo *_phaseOneSearchInfo;
    PhaseTwoSearchInfo *_phaseTwoSearchInfo;
    bool _useTruncateOptimizer;
    ClusterPartitionRanges _coveredRanges;
    std::vector<AttributeItemMapPtr> _globalVariables;
    std::vector<SortExprMeta> _sortExprMetaVec;
    // add for demo, unit:us(s*100000)
    int64_t _totalTime;
    uint8_t _serializeLevel;
    bool _lackResult;
    uint16_t _srcCount;
    MetaMap _metaMap;
private:
    HA3_LOG_DECLARE();
};

template<typename T>
std::vector<T*> Result::getGlobalVarialbes(const std::string& name) const {
    return getGlobalVarialbes<T>(name, _globalVariables);
}

template<typename T>
std::vector<T*> Result::getGlobalVarialbes(const std::string& name,
        const std::vector<AttributeItemMapPtr> &globalVariables)
{
    std::vector<T*> ret;
    for (size_t i = 0; i < globalVariables.size(); ++i) {
        AttributeItemMap::const_iterator it = globalVariables[i]->find(name);
        if (it == globalVariables[i]->end()) {
            continue;
        }
        AttributeItemTyped<T> *p = dynamic_cast<AttributeItemTyped<T> *>(it->second.get());
        if (p) {
            ret.push_back(p->getPointer());
        }
    }
    return ret;
}


typedef std::vector<ResultPtr> ResultPtrVector;

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RESULT_H
