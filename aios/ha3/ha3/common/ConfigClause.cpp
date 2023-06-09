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
#include "ha3/common/ConfigClause.h"

#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

#include "ha3/common/Tracer.h"

using namespace std;
using namespace autil;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, ConfigClause);

ConfigClause::ConfigClause() {
    init();
}

ConfigClause::~ConfigClause() {
}

const string &ConfigClause::getClusterName(size_t index) const {
    if (getClusterNameCount() == 0 || index >= getClusterNameCount()) {
        return HA3_EMPTY_STRING;
    }
    return _clusterNameVec[index];
}

void ConfigClause::addClusterName(const string &clusterName) {
    const string& zoneName = cluster2ZoneName(clusterName);
    if (find(_clusterNameVec.begin(), _clusterNameVec.end(), zoneName)
        == _clusterNameVec.end())
    {
        _clusterNameVec.push_back(zoneName);
    }
}

const vector<string>& ConfigClause::getClusterNameVector() const {
    return _clusterNameVec;
}

const string& ConfigClause::getFetchSummaryClusterName(const string& clusterName) const
{
    if (_fetchSummaryClusterName.empty()) {
        return clusterName;
    }
    return _fetchSummaryClusterName;
}

void ConfigClause::setFetchSummaryClusterName(const string& fetchSummaryCluster) {
    _fetchSummaryClusterName = cluster2ZoneName(fetchSummaryCluster);
}

uint8_t ConfigClause::getPhaseOneBasicInfoMask() const {
    return _phaseOneBasicInfoMask;
}
void ConfigClause::setPhaseOneBasicInfoMask(uint8_t phaseOneBasicInfoMask) {
    _phaseOneBasicInfoMask = phaseOneBasicInfoMask;
}

void ConfigClause::clear()
{
    init();
    _clusterNameVec.clear();
    _trace.clear();
    _rankTrace.clear();
    _kvPairs->clear();
}

void ConfigClause::init()
{
    _kvPairs.reset(new KeyValueMap());
    _startOffset = 0;
    _hitCount = 10;
    _rankSize = 0;
    _rerankSize = 0;
    _resultFormat = RESULT_FORMAT_XML;
    _phaseNumber = SEARCH_PHASE_ONE;
    _qrsChainName = DEFAULT_QRS_CHAIN;
    _summaryProfileName = DEFAULT_SUMMARY_PROFILE;
    _defaultOP = OP_UNKNOWN;
    _useCache = true;
    _allowLackOfSummary = true;
    _rpcTimeOut = 0;
    _seekTimeOut = 0;
    _displayCacheInfo = false;
    _rerankHint = false;
    _searcherReturnHits = 0;
    _noSummary = false;
    _doDedup = true;
    _batchScore = true;
    _optimizeRank = true;
    _optimizeComparator = true;
    _useTruncateOptimizer = true;
    _ignoreDelete = CB_DEFAULT;
    _compressType = autil::CompressType::INVALID_COMPRESS_TYPE;
    _innerResultCompressType = (uint32_t)autil::CompressType::NO_COMPRESS;
    _fetchSummaryType = BY_DOCID;
    _joinType = DEFAULT_JOIN;
    _protoFormatOption = 0;
    _actualHitsLimit = 0;
    _researchThreshold = 0;
    _phaseOneBasicInfoMask = 0;
    _subDocDisplayType = SUB_DOC_DISPLAY_NO;
    _version = multi_call::INVALID_VERSION_ID;

    _totalRankSize = 0;
    _totalRerankSize = 0;

    _getAllSubDoc = false;
}

void ConfigClause::clearClusterName() {
    _clusterNameVec.clear();
}

size_t ConfigClause::getClusterNameCount() const {
    return _clusterNameVec.size();
}

uint32_t ConfigClause::getStartOffset() const {
    return _startOffset;
}

void ConfigClause::setStartOffset(uint32_t startOffset) {
    _startOffset = startOffset;
}

uint32_t ConfigClause::getHitCount() const {
    return _hitCount;
}

void ConfigClause::setHitCount(uint32_t hitCount) {
    _hitCount = hitCount;
}

const string& ConfigClause::getResultFormatSetting() const {
    return _resultFormat;
}

void ConfigClause::setResultFormatSetting(const string &resultFormat) {
    _resultFormat = resultFormat;
}

const string &ConfigClause::getTrace() const {
	return _trace;
}

void ConfigClause::setTrace(const string &trace) {
	_trace = trace;
}

const string &ConfigClause::getRankTrace() const {
	return _rankTrace;
}

void ConfigClause::setRankTrace(const string &rankTrace) {
	_rankTrace = rankTrace;
}

bool ConfigClause::useCache() const {
    return _useCache;
}

void ConfigClause::setUseCacheConfig(const string &cacheConfg) {
    StringUtil::parseTrueFalse(cacheConfg, _useCache);
}

bool ConfigClause::allowLackOfSummary() const {
    return _allowLackOfSummary;
}

void ConfigClause::setAllowLackOfSummary(bool allowLackOfSummary) {
    _allowLackOfSummary = allowLackOfSummary;
}

void ConfigClause::setAllowLackSummaryConfig(
        const string &allowLackSummaryConfig)
{
    StringUtil::parseTrueFalse(allowLackSummaryConfig, _allowLackOfSummary);
}

bool ConfigClause::isNoSummary() const {
    return _noSummary;
}

bool ConfigClause::isOptimizeRank() const{
    return _optimizeRank;
}

void ConfigClause::setOptimizeRank(const string &optimizeRankConfig){
    StringUtil::parseTrueFalse(optimizeRankConfig, _optimizeRank);
}

bool ConfigClause::isOptimizeComparator() const{
    return _optimizeComparator;
}

void ConfigClause::setOptimizeComparator(const string &optimizeComparatorConfig){
    StringUtil::parseTrueFalse(optimizeComparatorConfig, _optimizeComparator);
}

void ConfigClause::setIgnoreDelete(bool flag) {
    _ignoreDelete = flag ? CB_TRUE : CB_FALSE;
}

void ConfigClause::setIgnoreDelete(const std::string &value) {
    bool ignoreDelete;
    if (autil::StringUtil::parseTrueFalse(value, ignoreDelete)) {
        setIgnoreDelete(ignoreDelete);
    }
}

ConfigClause::ConfigBool ConfigClause::ignoreDelete() const {
    return _ignoreDelete;
}

const string& ConfigClause::getDebugQueryKey() const {
    return _debugQueryKey;
}

void ConfigClause::setDebugQueryKey(const string &debugQueryKey){
    _debugQueryKey = debugQueryKey;
}

void ConfigClause::setCompressType(const string& compressTypeStr) {
    _compressType = autil::convertCompressType(compressTypeStr);
}

void ConfigClause::setInnerResultCompressType(const string& compressTypeStr) {
    autil::CompressType type = autil::convertCompressType(compressTypeStr);
    if (type != autil::CompressType::INVALID_COMPRESS_TYPE) {
        _innerResultCompressType = (uint32_t)type;
    }
}

void ConfigClause::setDoDedup(const string &dedup) {
    StringUtil::parseTrueFalse(dedup, _doDedup);
}

void ConfigClause::setNoSummary(const string &isNoSummary) {
    StringUtil::parseTrueFalse(isNoSummary, _noSummary);
}

void ConfigClause::setDisplayCacheInfo(const string &value) {
    StringUtil::parseTrueFalse(value, _displayCacheInfo);
}

void ConfigClause::setNeedRerank(const string &value) {
    StringUtil::parseTrueFalse(value, _rerankHint);
}

void ConfigClause::addKVPair(const string &key, const string &value) {
    (*_kvPairs)[key] = value;
}

void ConfigClause::removeKVPair(const string &key) {
    _kvPairs->erase(key);
}

const string &ConfigClause::getKVPairValue(const string &key) const {
    map<string, string>::const_iterator it = _kvPairs->find(key);
    if (it == _kvPairs->end()) {
        return HA3_EMPTY_STRING;
    }

    return it->second;
}

const map<string, string>& ConfigClause::getKVPairs() const  {
	return *_kvPairs;
}

uint32_t ConfigClause::getPhaseNumber() const {
    return _phaseNumber;
}

void ConfigClause::setPhaseNumber(uint32_t phaseNumber) {
    _phaseNumber = phaseNumber;
}

const string &ConfigClause::getQrsChainName() const {
    return _qrsChainName;
}

void ConfigClause::setQrsChainName(const string &qrsChainName) {
    _qrsChainName = qrsChainName;
}

const string &ConfigClause::getSummaryProfileName() const {
    return _summaryProfileName;
}

void ConfigClause::setSummaryProfileName(const string &summaryProfileName) {
    _summaryProfileName = summaryProfileName;
}

const string &ConfigClause::getDefaultIndexName() const {
    return _defaultIndexName;
}

void ConfigClause::setDefaultIndexName(const string &indexName) {
    _defaultIndexName = indexName;
}

QueryOperator ConfigClause::getDefaultOP() const {
    return _defaultOP;
}

void ConfigClause::setDefaultOP(const string &defaultOPStr) {
    if (defaultOPStr == "AND") {
        _defaultOP = OP_AND;
    } else if (defaultOPStr == "OR") {
        _defaultOP = OP_OR;
    }
}

void ConfigClause::addNoTokenizeIndex(const string &indexName) {
    _noTokenizeIndexes.insert(indexName);
}

const set<string>& ConfigClause::getNoTokenizeIndexes() const {
    return _noTokenizeIndexes;
}

void ConfigClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
    dataBuffer.write(_clusterNameVec);
    dataBuffer.write(_startOffset);
    dataBuffer.write(_hitCount);
    dataBuffer.write(_rankSize);
    dataBuffer.write(_rerankSize);
    dataBuffer.write(_trace);
    dataBuffer.write(_rankTrace);
    dataBuffer.write(*_kvPairs);
    dataBuffer.write(_phaseNumber);
    dataBuffer.write(_summaryProfileName);
    dataBuffer.write(_defaultIndexName);
    dataBuffer.write(_defaultOP);
    dataBuffer.write(_rpcTimeOut);
    dataBuffer.write(_seekTimeOut);
    dataBuffer.write(_globalAnalyzerName);
    dataBuffer.write(_searcherReturnHits);
    dataBuffer.write(_noSummary);
    dataBuffer.write(_doDedup);
    dataBuffer.write(_batchScore);
    dataBuffer.write(_sourceId);
    dataBuffer.write(_fetchSummaryType);
    dataBuffer.write(_joinType);
    dataBuffer.write(_innerResultCompressType);
    dataBuffer.write(_allowLackOfSummary);
    dataBuffer.write(_optimizeRank);
    dataBuffer.write(_optimizeComparator);
    dataBuffer.write(_noTokenizeIndexes);
    dataBuffer.write(_debugQueryKey);
    dataBuffer.write(_researchThreshold);
    dataBuffer.write(_useTruncateOptimizer);
    dataBuffer.write(_ignoreDelete);
    dataBuffer.write(_phaseOneBasicInfoMask);
    dataBuffer.write(_rerankHint);
    dataBuffer.write(_indexAnalyzerMap);
    dataBuffer.write(_requestTraceType);
    dataBuffer.write(_subDocDisplayType);
    dataBuffer.write(_totalRankSize);
    dataBuffer.write(_totalRerankSize);
    dataBuffer.write(_fetchSummaryGroup);
    dataBuffer.write(_hitSummarySchemaCacheKey);
    dataBuffer.write(_getAllSubDoc);
}

void ConfigClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
    dataBuffer.read(_clusterNameVec);
    dataBuffer.read(_startOffset);
    dataBuffer.read(_hitCount);
    dataBuffer.read(_rankSize);
    dataBuffer.read(_rerankSize);
    dataBuffer.read(_trace);
    dataBuffer.read(_rankTrace);
    _kvPairs.reset(new KeyValueMap());
    auto &kvPairs = *_kvPairs;
    dataBuffer.read(kvPairs);
    dataBuffer.read(_phaseNumber);
    dataBuffer.read(_summaryProfileName);
    dataBuffer.read(_defaultIndexName);
    dataBuffer.read(_defaultOP);
    dataBuffer.read(_rpcTimeOut);
    dataBuffer.read(_seekTimeOut);
    dataBuffer.read(_globalAnalyzerName);
    dataBuffer.read(_searcherReturnHits);
    dataBuffer.read(_noSummary);
    dataBuffer.read(_doDedup);
    dataBuffer.read(_batchScore);
    dataBuffer.read(_sourceId);
    dataBuffer.read(_fetchSummaryType);
    dataBuffer.read(_joinType);
    dataBuffer.read(_innerResultCompressType);
    dataBuffer.read(_allowLackOfSummary);
    dataBuffer.read(_optimizeRank);
    dataBuffer.read(_optimizeComparator);
    dataBuffer.read(_noTokenizeIndexes);
    dataBuffer.read(_debugQueryKey);
    dataBuffer.read(_researchThreshold);
    dataBuffer.read(_useTruncateOptimizer);
    dataBuffer.read(_ignoreDelete);
    dataBuffer.read(_phaseOneBasicInfoMask);
    dataBuffer.read(_rerankHint);
    dataBuffer.read(_indexAnalyzerMap);
    dataBuffer.read(_requestTraceType);
    dataBuffer.read(_subDocDisplayType);
    dataBuffer.read(_totalRankSize);
    dataBuffer.read(_totalRerankSize);
    dataBuffer.read(_fetchSummaryGroup);
    dataBuffer.read(_hitSummarySchemaCacheKey);
    dataBuffer.read(_getAllSubDoc);
}

Tracer* ConfigClause::createRequestTracer(const string &partionId, const string &address) const
{
    Tracer *tracer = NULL;
    int32_t level = Tracer::convertLevel(_trace);
    if (level < ISEARCH_TRACE_NONE ) {
        Tracer::TraceMode mode = _requestTraceType == "kvtracer" ?
                                 Tracer::TM_TREE : Tracer::TM_VECTOR;
        tracer = new Tracer(mode);
        tracer->setPartitionId(partionId);
        tracer->setLevel(level);
        tracer->setAddress(address);
    }
    return tracer;
}

uint32_t ConfigClause::getSearcherReturnHits() const {
    return _searcherReturnHits;
}

void ConfigClause::setSearcherReturnHits(uint32_t searcherReturnHits) {
    _searcherReturnHits = searcherReturnHits;
}

void ConfigClause::setBatchScore(const string &batchScore) {
    StringUtil::parseTrueFalse(batchScore, _batchScore);
}

uint32_t ConfigClause::getSearcherRequireCount() const {
    if (_searcherReturnHits != 0) {
        return min(_searcherReturnHits, _startOffset + _hitCount);
    } else {
        return _startOffset + _hitCount;
    }
}

string ConfigClause::toString() const {
    return _originalString;
}

void ConfigClause::setMetricsSrc(const string &metricsStr) {
    _metricsSrc = metricsStr;
}

const string &ConfigClause::getMetricsSrc() const {
    return _metricsSrc;
}

bool ConfigClause::getAllSubDoc() const {
    return _getAllSubDoc;
}

void ConfigClause::setGetAllSubDoc(const std::string &getAllSubDoc) {
    StringUtil::parseTrueFalse(getAllSubDoc, _getAllSubDoc);
}

} // namespace common
} // namespace isearch
