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
#ifndef __INDEXER_DEFINE_H
#define __INDEXER_DEFINE_H

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib {

class IndexerUserData;
typedef std::shared_ptr<IndexerUserData> IndexerUserDataPtr;

class IndexerResource
{
public:
    IndexerResource() {}
    IndexerResource(const config::IndexPartitionSchemaPtr& _schema, const config::IndexPartitionOptions& _options,
                    const util::CounterMapPtr& _counterMap, const index_base::PartitionMeta& _partitionMeta,
                    const std::string& _indexName, const std::string& _pluginPath,
                    util::MetricProviderPtr _metricProvider = nullptr,
                    const IndexerUserDataPtr& _userData = IndexerUserDataPtr())
        : schema(_schema)
        , options(_options)
        , partitionMeta(_partitionMeta)
        , indexName(_indexName)
        , pluginPath(_pluginPath)
        , metricProvider(_metricProvider)
        , mCounterMap(_counterMap)
        , mUserData(_userData)
    {
    }

public:
    util::AccumulativeCounterPtr GetAccCounter(const std::string& nodePath)
    {
        return mCounterMap->GetAccCounter(GetCounterName(nodePath));
    }

    util::StateCounterPtr GetStateCounter(const std::string& nodePath)
    {
        return mCounterMap->GetStateCounter(GetCounterName(nodePath));
    }

    const IndexerUserDataPtr& GetUserData() const { return mUserData; }

public:
    const config::IndexPartitionSchemaPtr schema;
    const config::IndexPartitionOptions options;
    const index_base::PartitionMeta partitionMeta;
    std::string indexName;
    std::string pluginPath;
    util::MetricProviderPtr metricProvider;

private:
    std::string GetCounterName(const std::string& nodePath) const { return indexName + ".user." + nodePath; }

private:
    const util::CounterMapPtr mCounterMap;
    IndexerUserDataPtr mUserData;
};

typedef std::shared_ptr<IndexerResource> IndexerResourcePtr;

class DocIdMap
{
public:
    DocIdMap(docid_t baseDocId, const index::ReclaimMapPtr& reclaimMap)
        : mSegBaseDocId(baseDocId)
        , mReclaimMap(reclaimMap)
    {
    }

    ~DocIdMap() {};

    DocIdMap(const DocIdMap&) = default;
    DocIdMap& operator=(const DocIdMap&) = delete;

public:
    docid_t GetNewDocId(docid_t oldDocId) const
    {
        assert(mReclaimMap.get());
        docid_t oldGlobalDocId = mSegBaseDocId + oldDocId;
        return mReclaimMap->GetNewId(oldGlobalDocId);
    }
    std::pair<segmentindex_t, docid_t> GetNewLocalId(docid_t oldDocId) const
    {
        assert(mReclaimMap.get());
        docid_t oldGlobalDocId = mSegBaseDocId + oldDocId;
        return mReclaimMap->GetNewLocalId(oldGlobalDocId);
    }
    std::pair<segmentindex_t, docid_t> GetLocalId(docid_t newDocId) const
    {
        assert(mReclaimMap.get());
        return mReclaimMap->GetLocalId(newDocId);
    }

    size_t GetTotalDocCount() const
    {
        assert(mReclaimMap);
        return mReclaimMap->GetTotalDocCount();
    }
    size_t GetNewDocCount(segmentindex_t segmentIndex) const
    {
        assert(mReclaimMap);
        return mReclaimMap->GetTargetSegmentDocCount(segmentIndex);
    }

private:
    docid_t mSegBaseDocId;
    const index::ReclaimMapPtr& mReclaimMap;
};

class MatchInfo
{
public:
    MatchInfo() : pool(nullptr), matchCount(0), docIds(NULL), matchValues(NULL), type(mv_unknown) {};

    MatchInfo(MatchInfo&& other)
        : pool(other.pool)
        , matchCount(other.matchCount)
        , docIds(other.docIds)
        , matchValues(other.matchValues)
        , type(other.type)
    {
        other.pool = nullptr;
        other.docIds = nullptr;
        other.matchValues = nullptr;
    }

    ~MatchInfo()
    {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, docIds, matchCount);
        IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, matchValues, matchCount);
    }

public:
    MatchInfo(const MatchInfo& other) = delete;
    MatchInfo& operator=(const MatchInfo& other) = delete;

public:
    MatchInfo& operator=(MatchInfo&& other)
    {
        std::swap(pool, other.pool);
        std::swap(matchCount, other.matchCount);
        std::swap(docIds, other.docIds);
        std::swap(matchValues, other.matchValues);
        std::swap(type, other.type);
        return *this;
    }

public:
    autil::mem_pool::Pool* pool;
    size_t matchCount;
    docid_t* docIds;
    matchvalue_t* matchValues;
    MatchValueType type;
};
typedef std::shared_ptr<MatchInfo> MatchInfoPtr;

class SegmentMatchInfo
{
public:
    SegmentMatchInfo() : baseDocId(0) {}

    SegmentMatchInfo(const SegmentMatchInfo& other) : baseDocId(other.baseDocId), matchInfo(other.matchInfo) {}

public:
    SegmentMatchInfo& operator=(SegmentMatchInfo& other)
    {
        baseDocId = other.baseDocId;
        matchInfo = other.matchInfo;
        return *this;
    }

    docid_t GetLastMatchDocId() const
    {
        return matchInfo->matchCount == 0 ? INVALID_DOCID : baseDocId + matchInfo->docIds[matchInfo->matchCount - 1];
    }

public:
    docid_t baseDocId;
    MatchInfoPtr matchInfo;
};

} // namespace indexlib
#endif
