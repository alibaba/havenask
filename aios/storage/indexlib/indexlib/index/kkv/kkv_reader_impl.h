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

#include <memory>

#include "autil/EnvUtil.h"
#include "indexlib/index/kkv/kkv_reader_impl_base.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class KKVReaderImpl final : public KKVReaderImplBase
{
public:
    KKVReaderImpl(const std::string& schemaName = "");
    ~KKVReaderImpl() {}

private:
    typedef BuildingKKVSegmentReader<SKeyType> BuildingSegReaderTyped;
    typedef std::shared_ptr<BuildingSegReaderTyped> BuildingSegReaderTypedPtr;
    typedef BuiltKKVSegmentReader<SKeyType> BuiltSegReaderTyped;
    DEFINE_SHARED_PTR(BuiltSegReaderTyped);
    typedef std::vector<BuiltSegReaderTypedPtr> BuiltSegmentReaders;
    typedef std::vector<BuiltSegmentReaders> ColumnIdToSegmentReaders;
    typedef std::vector<BuildingSegReaderTypedPtr> BuildingSegmentReaders;
    typedef std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                               autil::mem_pool::pool_allocator<SKeyType>>
        SKeyUnorderedSet;
    typedef NormalKKVIteratorImpl<SKeyType> SeekSkeyKKVIteratorImpl;
    typedef NormalKKVIteratorImpl<SKeyType> NoSeekSkeyKKVIteratorImpl;

private:
    bool LoadBuiltSegments(const config::KKVIndexConfigPtr& kkvConfig,
                           const index_base::PartitionDataPtr& partitionData,
                           ColumnIdToSegmentReaders& columnIdToSegReaders);

    void AddBuildingSegmentReaders(const index_base::PartitionDataPtr& partitionData);

    void InitRecorder();

    template <typename Alloc = std::allocator<uint64_t>>
    FL_LAZY(KKVIterator*)
    InnerLookup(KKVIndexOptions* indexOptions, PKeyType pKey, const std::vector<uint64_t, Alloc>& suffixKeyHashVec,
                uint64_t timestamp, TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
                KVMetricsCollector* metricsCollector);

protected:
    bool doCollectAllCode() override;

public:
    void Open(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partitionData,
              const util::SearchCachePartitionWrapperPtr& searchCache) override;
    FL_LAZY(KKVIterator*)
    Lookup(KKVIndexOptions* indexOptions, PKeyType pkeyHash, std::vector<uint64_t> suffixKeyHashVec, uint64_t timestamp,
           TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
           KVMetricsCollector* metricsCollector = nullptr) override
    {
        FL_CORETURN FL_COAWAIT InnerLookup(indexOptions, pkeyHash, suffixKeyHashVec, timestamp, searchCacheType, pool,
                                           metricsCollector);
    }

    FL_LAZY(KKVIterator*)
    Lookup(KKVIndexOptions* indexOptions, PKeyType pkeyHash,
           std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashVec, uint64_t timestamp,
           TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
           KVMetricsCollector* metricsCollector = nullptr) override
    {
        FL_CORETURN FL_COAWAIT InnerLookup(indexOptions, pkeyHash, suffixKeyHashVec, timestamp, searchCacheType, pool,
                                           metricsCollector);
    }
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    ColumnIdToSegmentReaders mColumnIdToSegmentReaders;
    BuildingSegmentReaders mBuildingSegReaders;
    util::SearchCachePartitionWrapperPtr mSearchCache;
    config::KKVIndexConfigPtr mDataKKVConfig;
    index_base::PartitionDataPtr mPartitionData;

    std::unique_ptr<KKVMetricsRecorder> mRecorder;

    // todo codegen
    bool mHasSegReader;
    bool mHasSearchCache;
    bool mHasBuildingSegReader;

private:
    friend class KKVReaderTest;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, KKVReaderImpl);

///////////////////////////////////////////

template <typename SKeyType>
inline KKVReaderImpl<SKeyType>::KKVReaderImpl(const std::string& schemaName) : KKVReaderImplBase(schemaName)
{
    mHasSegReader = false;
    mHasSearchCache = false;
    mHasBuildingSegReader = false;
}

template <typename SKeyType>
inline void KKVReaderImpl<SKeyType>::Open(const config::KKVIndexConfigPtr& kkvConfig,
                                          const index_base::PartitionDataPtr& partitionData,
                                          const util::SearchCachePartitionWrapperPtr& searchCache)
{
    mSearchCache = searchCache;
    ColumnIdToSegmentReaders columnIdToSegReaders;
    if (!LoadBuiltSegments(kkvConfig, partitionData, columnIdToSegReaders)) {
        IE_LOG(ERROR, "Open loadSegments FAILED");
        return;
    }
    mColumnIdToSegmentReaders.swap(columnIdToSegReaders);
    AddBuildingSegmentReaders(partitionData);
    mSKeyDictFieldType = GetSKeyDictKeyType(kkvConfig->GetSuffixFieldConfig()->GetFieldType());
    mDataKKVConfig = kkvConfig;
    mPartitionData = partitionData;
    mHasSegReader = mBuildingSegReaders.size() > 0 || mColumnIdToSegmentReaders.size() > 0;
    mHasSearchCache = searchCache.operator bool();
    mHasBuildingSegReader = mBuildingSegReaders.size() > 0;
    InitRecorder();
}

template <typename SKeyType>
template <typename Alloc>
inline FL_LAZY(KKVIterator*) KKVReaderImpl<SKeyType>::InnerLookup(KKVIndexOptions* indexOptions, PKeyType pKey,
                                                                  const std::vector<uint64_t, Alloc>& suffixKeyHashVec,
                                                                  uint64_t timestamp,
                                                                  TableSearchCacheType searchCacheType,
                                                                  autil::mem_pool::Pool* pool,
                                                                  KVMetricsCollector* metricsCollector)
{
    assert(indexOptions);
    if (!mHasSegReader) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }

    uint64_t currentTimeInSecond = MicrosecondToSecond(timestamp);
    size_t columnId = util::ColumnUtil::GetColumnId(pKey, mColumnIdToSegmentReaders.size());
    const auto& columnSegReaders = mColumnIdToSegmentReaders[columnId];
    if (!mHasBuildingSegReader && columnSegReaders.empty()) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN nullptr;
    }

    bool onlyCache = (searchCacheType == tsc_only_cache);
    if (!mHasSearchCache && onlyCache) {
        IE_LOG(WARN, "can not search only cache without search cache!");
        onlyCache = false;
    }

    KKVIteratorImplBase* iter = nullptr;
    auto keyHash = indexOptions->GetLookupKeyHash(pKey);
    if ((!mHasSearchCache || searchCacheType == tsc_no_cache) && !future_lite::interface::USE_COROUTINES) {
        if (suffixKeyHashVec.size() > 0) {
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(pool, SeekSkeyKKVIteratorImpl, pool, indexOptions, keyHash,
                                                std::move(suffixKeyHashVec), columnSegReaders, mBuildingSegReaders,
                                                currentTimeInSecond, metricsCollector);
        } else {
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(pool, NoSeekSkeyKKVIteratorImpl, pool, indexOptions, keyHash,
                                                std::move(suffixKeyHashVec), columnSegReaders, mBuildingSegReaders,
                                                currentTimeInSecond, metricsCollector);
        }
    } else {
        static util::SearchCachePartitionWrapperPtr emptySearchCache;
        iter = FL_COAWAIT KKVSearchCoroutine<SKeyType>::Search(
            pool, indexOptions, keyHash, std::move(suffixKeyHashVec), columnSegReaders, mBuildingSegReaders,
            (searchCacheType == tsc_no_cache) ? emptySearchCache : mSearchCache, currentTimeInSecond, onlyCache,
            metricsCollector, mRecorder.get());
    }
    KKVIterator* kkvIter = IE_POOL_COMPATIBLE_NEW_CLASS(
        pool, KKVIterator, pool, iter, mSKeyDictFieldType, indexOptions->kkvConfig.get(), metricsCollector,
        indexOptions->plainFormatEncoder.get(), indexOptions->GetSKeyCountLimits());
    FL_CORETURN kkvIter;
}

template <typename SKeyType>
inline bool KKVReaderImpl<SKeyType>::LoadBuiltSegments(const config::KKVIndexConfigPtr& kkvConfig,
                                                       const index_base::PartitionDataPtr& partitionData,
                                                       ColumnIdToSegmentReaders& columnIdToSegReaders)
{
    typedef std::map<segmentid_t, BuiltSegReaderTypedPtr> SegIdToReaderMap;
    SegIdToReaderMap segIdToReaderMap;

    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr builtSegIter = segIter->CreateIterator(index_base::SIT_BUILT);
    assert(builtSegIter);

    while (builtSegIter->IsValid()) {
        index_base::SegmentData segData = builtSegIter->GetSegmentData();
        if (segData.GetSegmentInfo()->docCount == 0) {
            builtSegIter->MoveToNext();
            continue;
        }
        BuiltSegReaderTypedPtr segmentReader(new BuiltSegReaderTyped);
        try {
            segmentReader->Open(kkvConfig, segData);
        } catch (const util::ExceptionBase& e) {
            IE_LOG(ERROR, "Load segment [%s] FAILED, reason: [%s]", segData.GetDirectory()->DebugString().c_str(),
                   e.what());
            throw;
        }
        segIdToReaderMap[segData.GetSegmentId()] = segmentReader;
        builtSegIter->MoveToNext();
    }
    index_base::Version version = partitionData->GetVersion();
    const indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
    if (levelInfo.GetTopology() == indexlibv2::framework::topo_sequence && levelInfo.GetLevelCount() > 1) {
        IE_LOG(ERROR, "kkv reader does not support sequence topology with more than one level");
        return false;
    }

    size_t columnCount = levelInfo.GetShardCount();
    columnIdToSegReaders.clear();
    columnIdToSegReaders.resize(columnCount);
    for (size_t i = 0; i < columnCount; ++i) {
        std::vector<segmentid_t> segIds = levelInfo.GetSegmentIds(i);
        // TODO make columnreaders ordered new to old, need change iterator
        for (auto iter = segIds.crbegin(); iter != segIds.crend(); ++iter) {
            typename SegIdToReaderMap::const_iterator readerIter = segIdToReaderMap.find(*iter);
            if (readerIter != segIdToReaderMap.end()) {
                columnIdToSegReaders[i].push_back(readerIter->second);
            }
        }
    }
    return true;
}

template <typename SKeyType>
inline void KKVReaderImpl<SKeyType>::AddBuildingSegmentReaders(const index_base::PartitionDataPtr& partitionData)
{
    index_base::SegmentIteratorPtr buildingSegIter =
        partitionData->CreateSegmentIterator()->CreateIterator(index_base::SIT_BUILDING);
    assert(buildingSegIter);
    while (buildingSegIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        assert(inMemSegment);
        const index_base::InMemorySegmentReaderPtr& reader = inMemSegment->GetSegmentReader();
        assert(reader);
        BuildingSegReaderTypedPtr inMemSegReader =
            DYNAMIC_POINTER_CAST(BuildingSegReaderTyped, reader->GetKKVSegmentReader());
        mBuildingSegReaders.insert(mBuildingSegReaders.begin(), inMemSegReader);
        buildingSegIter->MoveToNext();
    }
}

template <typename SKeyType>
inline void KKVReaderImpl<SKeyType>::InitRecorder()
{
    // FIXME: do not get envs here!
    bool enable = autil::EnvUtil::getEnv<int>("ENABLE_INDEXLIB_KKV_RECORDER", 1);
    if (!enable) {
        return;
    }
    std::string serviceName = autil::EnvUtil::getEnv<std::string>("INDEXLIB_RECORDER_NAME", "indexlib");
    auto monitor = kmonitor_adapter::MonitorFactory::getInstance()->createMonitor(serviceName);
    if (!monitor) {
        IE_LOG(WARN, "create monitor [%s] failed", serviceName.c_str());
        return;
    }
    mRecorder.reset(new KKVMetricsRecorder(monitor, mSchemaName));
}

template <typename SKeyType>
inline bool KKVReaderImpl<SKeyType>::doCollectAllCode()
{
    INIT_CODEGEN_INFO(KKVReaderImpl, false);
    COLLECT_CONST_MEM_VAR(mHasSegReader);
    COLLECT_CONST_MEM_VAR(mHasSearchCache);
    COLLECT_CONST_MEM_VAR(mHasBuildingSegReader);
    for (size_t i = 0; i < mColumnIdToSegmentReaders.size(); i++) {
        if (mColumnIdToSegmentReaders[i].size() > 0) {
            mColumnIdToSegmentReaders[i][0]->collectAllCode();
            combineCodegenInfos(mColumnIdToSegmentReaders[i][0].get());
            break;
        }
    }
    std::string envParam = autil::EnvUtil::getEnv("READ_REPORT_METRICS");
    bool KKVReportMetrics = true;
    if (envParam == "false") {
        KKVReportMetrics = false;
    }
    {
        codegen::AllCodegenInfo codegenInfos;

        NormalKKVIteratorImpl<SKeyType>::CollectAllCode(KKVReportMetrics, false, mColumnIdToSegmentReaders,
                                                        mBuildingSegReaders, codegenInfos);
        COLLECT_TYPE_REDEFINE(NoSeekSkeyKKVIteratorImpl,
                              codegenInfos.GetTargetClassName("NormalKKVIteratorImpl") + "<SKeyType>");
        combineCodegenInfos(codegenInfos);
    }
    {
        codegen::AllCodegenInfo codegenInfos;
        NormalKKVIteratorImpl<SKeyType>::CollectAllCode(KKVReportMetrics, true, mColumnIdToSegmentReaders,
                                                        mBuildingSegReaders, codegenInfos);
        COLLECT_TYPE_REDEFINE(SeekSkeyKKVIteratorImpl,
                              codegenInfos.GetTargetClassName("NormalKKVIteratorImpl") + "<SKeyType>");
        combineCodegenInfos(codegenInfos);
    }
    return true;
}

template <typename SKeyType>
inline void KKVReaderImpl<SKeyType>::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mHasSegReader);
    COLLECT_CONST_MEM(checker, mHasSearchCache);
    COLLECT_CONST_MEM(checker, mHasBuildingSegReader);
    COLLECT_TYPE_DEFINE(checker, NoSeekSkeyKKVIteratorImpl);
    COLLECT_TYPE_DEFINE(checker, SeekSkeyKKVIteratorImpl);

    for (size_t i = 0; i < mColumnIdToSegmentReaders.size(); i++) {
        if (mColumnIdToSegmentReaders[i].size() > 0) {
            mColumnIdToSegmentReaders[i][0]->TEST_collectCodegenResult(checkers, id);
            break;
        }
    }
    checkers["KKVReaderImpl"] = checker;
}
}} // namespace indexlib::index
