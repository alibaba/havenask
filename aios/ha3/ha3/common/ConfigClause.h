/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/CompressionUtil.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class ConfigClause : public ClauseBase
{
public:
    typedef std::map<std::string, std::string> IndexAnalyzerMap;
    enum ConfigBool {
        CB_DEFAULT,
        CB_TRUE,
        CB_FALSE
    };
public:
    ConfigClause();
    ~ConfigClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    const std::string &getClusterName(size_t index = 0) const;
    const std::vector<std::string> &getClusterNameVector() const;

    void clear();
    void clearClusterName();
    size_t getClusterNameCount() const;
    void addClusterName(const std::string &clusterName);

    const std::string& getFetchSummaryClusterName(const std::string &clusterName) const;
    void setFetchSummaryClusterName(const std::string& fetchSummaryCluster);

    uint8_t getPhaseOneBasicInfoMask() const;
    void setPhaseOneBasicInfoMask(uint8_t phaseOneBasicInfoMask);

    uint32_t getStartOffset() const;
    void setStartOffset(uint32_t startOffset);

    uint32_t getHitCount() const;
    void setHitCount(uint32_t hitCount);

    uint32_t getSearcherRequireCount() const;

    uint32_t getRankSize() const {return _rankSize;}
    void setRankSize(uint32_t s) {_rankSize = s;}

    uint32_t getTotalRankSize() const {return _totalRankSize;}
    void setTotalRankSize(uint32_t s) {_totalRankSize = s;}

    uint32_t getRerankSize() const {return _rerankSize;}
    void setRerankSize(uint32_t s) {_rerankSize = s;}

    uint32_t getTotalRerankSize() const {return _totalRerankSize;}
    void setTotalRerankSize(uint32_t s) {_totalRerankSize = s;}

    const std::string& getResultFormatSetting() const;
    void setResultFormatSetting(const std::string &resultFormat);

    const std::string &getTrace() const;
    void setTrace(const std::string &trace);
    common::Tracer* createRequestTracer(const std::string &partionId,
            const std::string &address) const;

    const std::string &getRankTrace() const;
    void setRankTrace(const std::string &rankTrace);

    bool useCache() const;
    void setUseCacheConfig(const std::string &cacheConfg);

    bool allowLackOfSummary() const;
    void setAllowLackOfSummary(bool allowLackOfSummary);
    void setAllowLackSummaryConfig(const std::string &allowLackSummaryConfig);

    bool isNoSummary() const;
    void setNoSummary(const std::string &isNoSummary);

    bool isOptimizeRank() const;
    void setOptimizeRank(const std::string &optimizeRankConfig);

    bool isOptimizeComparator() const;
    void setOptimizeComparator(const std::string &optimizeComparatorConfig);

    const std::string& getDebugQueryKey()const;
    void setDebugQueryKey(const std::string &debugQueryKey);

    void setDoDedup(bool f) {_doDedup = f;}
    bool isDoDedup() const {return _doDedup;}
    void setDoDedup(const std::string &dedup);

    void addKVPair(const std::string &key, const std::string &value);
    void removeKVPair(const std::string &key);
    const std::string &getKVPairValue(const std::string &key) const;
    const std::map<std::string, std::string> &getKVPairs() const;
    KeyValueMapPtr getKeyValueMapPtr() const {
        return _kvPairs;
    }

    uint32_t getPhaseNumber() const;
    void setPhaseNumber(uint32_t phaseNumber);

    const std::string &getQrsChainName() const;
    void setQrsChainName(const std::string &qrsChainName);

    const std::string &getSummaryProfileName() const;
    void setSummaryProfileName(const std::string &summaryProfileName);

    const std::string &getDefaultIndexName() const;
    void setDefaultIndexName(const std::string &indexName);

    QueryOperator getDefaultOP() const;
    void setDefaultOP(const std::string &defaultOPStr);

    void setRpcTimeOut(uint32_t timeOut) {
        _rpcTimeOut = timeOut;
    }

    uint32_t getRpcTimeOut() const {return _rpcTimeOut;}

    void setSeekTimeOut(uint32_t timeOut) {
        _seekTimeOut = timeOut;
    }

    uint32_t getSeekTimeOut() const {return _seekTimeOut;}

    void setSourceId(const std::string &sourceId) {
        _sourceId = sourceId;
    }
    const std::string& getSourceId() const {
        return _sourceId;
    }

    void setPhaseOneTaskQueueName(const std::string &phaseOneTaskQueueName) {
        _phaseOneTaskQueueName = phaseOneTaskQueueName;
    }
    const std::string& getPhaseOneTaskQueueName() const {
        return _phaseOneTaskQueueName;
    }

    void setPhaseTwoTaskQueueName(const std::string &phaseTwoTaskQueueName) {
        _phaseTwoTaskQueueName = phaseTwoTaskQueueName;
    }
    const std::string& getPhaseTwoTaskQueueName() const {
        return _phaseTwoTaskQueueName;
    }

    const std::string &getAnalyzerName() const { return _globalAnalyzerName; }
    void setAnalyzerName(const std::string &analyzerName) {
        _globalAnalyzerName = analyzerName;
    }

    bool getDisplayCacheInfo() const {return _displayCacheInfo;}
    void setDisplayCacheInfo(const std::string &value);

    bool needRerank() const { return _rerankHint; }
    void setNeedRerank(const std::string &value);
    void setNeedRerank(bool need) {
        _rerankHint = need;
    }

    uint32_t getSearcherReturnHits() const;
    void setSearcherReturnHits(uint32_t searcherReturnHits);

    void getCompressType(autil::CompressType& compressType) const {
        if (_compressType != autil::CompressType::INVALID_COMPRESS_TYPE) {
            compressType = _compressType;
        }
    }
    void setCompressType(const std::string& compressTypeStr);

    autil::CompressType getInnerResultCompressType() const {
        return (autil::CompressType)_innerResultCompressType;
    }
    void setInnerResultCompressType(const std::string& compressTypeStr);

    uint32_t getFetchSummaryType() const {
        return _fetchSummaryType;
    }
    void setFetchSummaryType(uint32_t fetchSummaryType) {
        _fetchSummaryType = fetchSummaryType;
    }
    const std::string& getFetchSummaryGroup() const {
        return _fetchSummaryGroup;
    }
    void setFetchSummaryGroup(const std::string& fetchSummaryGroup) {
        _fetchSummaryGroup = fetchSummaryGroup;
    }
    JoinType getJoinType() const {
        return _joinType;
    }
    void setJoinType(JoinType type) {
        _joinType = type;
    }
    SubDocDisplayType getSubDocDisplayType() const {
        return _subDocDisplayType;
    }
    void setSubDocDisplayType(SubDocDisplayType type) {
        _subDocDisplayType = type;
    }

    void addProtoFormatOption(uint32_t option) {
        _protoFormatOption |= option;
    }
    void setProtoFormatOption(uint32_t option) {
        _protoFormatOption = option;
    }

    uint32_t getProtoFormatOption() const {
        return _protoFormatOption;
    }

    bool hasPBMatchDocFormat() const {
        return _protoFormatOption & PBResultType::PB_MATCHDOC_FORMAT;
    }

    uint32_t getActualHitsLimit() const {return _actualHitsLimit;}
    void setActualHitsLimit(uint32_t s) {_actualHitsLimit = s;}

    bool isBatchScore() const { return _batchScore; }
    void setBatchScore(const std::string &batchScore);

    void addNoTokenizeIndex(const std::string &indexName);
    const std::set<std::string>& getNoTokenizeIndexes() const;
    bool isIndexNoTokenize(const std::string &indexName) const {
        return _noTokenizeIndexes.find(indexName) != _noTokenizeIndexes.end();
    }

    void setResearchThreshold(uint32_t threshold) {
        _researchThreshold = threshold;
    }
    uint32_t getResearchThreshold() const {
        return _researchThreshold;
    }

    void setUseTruncateOptimizer(bool flag) {
        _useTruncateOptimizer = flag;
    }
    bool useTruncateOptimizer() const {
        return _useTruncateOptimizer;
    }

    void setIgnoreDelete(bool flag);
    void setIgnoreDelete(const std::string &value);
    ConfigBool ignoreDelete() const;

    void setMetricsSrc(const std::string &metricsStr);
    const std::string &getMetricsSrc() const;

    void addIndexAnalyzerName(const std::string& indexName,
                              const std::string& analyzerName) {
        _indexAnalyzerMap[indexName] = analyzerName;
    }
    std::string getIndexAnalyzerName(const std::string& indexName) const {
        IndexAnalyzerMap::const_iterator it = _indexAnalyzerMap.find(indexName);
        if (it != _indexAnalyzerMap.end()) {
            return it->second;
        }
        return _globalAnalyzerName;
    }
    const IndexAnalyzerMap& getIndexAnalyzerMap() const {
        return _indexAnalyzerMap;
    }
    void setTraceType(const std::string &traceType) {
        _requestTraceType = traceType;
    }
    const std::string &getTraceType() const {
        return _requestTraceType;
    }
    multi_call::VersionTy getVersion() const {
        return _version;
    }
    void setVersion(multi_call::VersionTy version) {
        _version = version;
    }
    const std::string& getHitSummarySchemaCacheKey() const {
        return _hitSummarySchemaCacheKey;
    }
    void setHitSummarySchemaCacheKey(const std::string& key) {
        _hitSummarySchemaCacheKey = key;
    }
    bool getAllSubDoc() const;
    void setGetAllSubDoc(const std::string &getAllSubDoc);

private:
    void init();
private:
    //Several cluster names can be indicated in config clause .
    std::vector<std::string> _clusterNameVec;
    uint32_t _startOffset;
    uint32_t _hitCount;
    //optional
    uint32_t _rankSize;
    uint32_t _rerankSize;
    std::string _resultFormat;
    std::string _trace;
    std::string _rankTrace;
    KeyValueMapPtr _kvPairs;
    uint32_t _phaseNumber;
    std::string _qrsChainName;
    std::string _summaryProfileName;
    std::string _defaultIndexName;
    QueryOperator _defaultOP;
    bool _useCache;
    bool _allowLackOfSummary;
    bool _noSummary;
    bool _doDedup;
    bool _batchScore;
    bool _optimizeRank;
    bool _optimizeComparator; //used in expression comparator, lazy scoring  for the second expression value
    bool _useTruncateOptimizer;
    ConfigBool _ignoreDelete;
    std::string _debugQueryKey;
    uint32_t _rpcTimeOut;
    uint32_t _seekTimeOut;
    bool _displayCacheInfo;
    bool _rerankHint;
    uint32_t _searcherReturnHits;
    autil::CompressType _compressType;
    uint32_t _innerResultCompressType;
    std::string _sourceId;
    uint32_t _fetchSummaryType;
    uint32_t _protoFormatOption;
    uint32_t _actualHitsLimit;
    std::string _phaseOneTaskQueueName;
    std::string _phaseTwoTaskQueueName;
    // research related
    uint32_t _researchThreshold;
    std::string _fetchSummaryClusterName;
    uint8_t _phaseOneBasicInfoMask;
    std::string _metricsSrc;
    std::string _globalAnalyzerName;
    std::set<std::string> _noTokenizeIndexes;
    IndexAnalyzerMap _indexAnalyzerMap;
    std::string _requestTraceType;

    SubDocDisplayType _subDocDisplayType;
    JoinType _joinType;
    multi_call::VersionTy _version;

    // incompatible field add here, move to databuffer serialize after incompatible update
    uint32_t _totalRankSize;
    uint32_t _totalRerankSize;
    std::string _fetchSummaryGroup;
    std::string _hitSummarySchemaCacheKey;
    bool _getAllSubDoc;

private:
    friend class ConfigClauseTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ConfigClause> ConfigClausePtr;

} // namespace common
} // namespace isearch
