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
#include "indexlib/index/kv/kv_reader_impl.h"

using namespace std;
using namespace indexlib::codegen;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVReaderImpl);

void KVReaderImpl::Open(const config::KVIndexConfigPtr& kvIndexConfig,
                        const index_base::PartitionDataPtr& partitionData,
                        const util::SearchCachePartitionWrapperPtr& searchCache, int64_t latestIncSkipTs)
{
    KVReader::Open(kvIndexConfig, partitionData, searchCache, latestIncSkipTs);
    char* envParam = getenv("READ_REPORT_METRICS");
    if (envParam && string(envParam) == "false") {
        mKVReportMetrics = false;
    }
    if (kvIndexConfig && kvIndexConfig->GetRegionCount() > 1) {
        mHasMultiRegion = true;
    }
    mHasTTL = kvIndexConfig->TTLEnabled();
    mSearchCache = searchCache;
    if (mSearchCache) {
        mHasSearchCache = true;
    }
    LoadSegmentReaders(partitionData, kvIndexConfig);
}

inline void KVReaderImpl::LoadSegmentReaders(const index_base::PartitionDataPtr& partitionData,
                                             const config::KVIndexConfigPtr& kvIndexConfig)
{
    // todo online offline to one segment reader vector
    index_base::Version onDiskVersion = partitionData->GetOnDiskVersion();
    LoadSegmentReaders(partitionData, kvIndexConfig, onDiskVersion, mOfflineSegmentReaders);
    index_base::OnlineSegmentDirectoryPtr onlineSegDir =
        DYNAMIC_POINTER_CAST(index_base::OnlineSegmentDirectory, partitionData->GetSegmentDirectory());
    if (onlineSegDir) {
        const auto& rtSegDir = onlineSegDir->GetRtSegmentDirectory();
        assert(rtSegDir);
        index_base::Version rtVersion = rtSegDir->GetVersion();
        LoadOnlineSegmentReaders(partitionData, kvIndexConfig, rtVersion, mOnlineSegmentReaders);
    }
    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    index_base::SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(index_base::SIT_BUILDING);
    assert(buildingSegIter);
    while (buildingSegIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        assert(inMemSegment);
        OnlineSegmentReader onlineReader;
        onlineReader.Open(kvIndexConfig, inMemSegment->GetSegmentData());
        mOnlineSegmentReaders.insert(mOnlineSegmentReaders.begin(), onlineReader);
        mHasBuildingSegment = true;
        mBuildingSegmentId = buildingSegIter->GetSegmentId();
        buildingSegIter->MoveToNext();
    }
    if (mBuildingSegmentId == INVALID_SEGMENTID) {
        mBuildingSegmentId = segIter->GetNextBuildingSegmentId();
    }
    if (mOnlineSegmentReaders.empty()) {
        mHasOnlineSegment = false;
    }
}

void KVReaderImpl::LoadOnlineSegmentReaders(const index_base::PartitionDataPtr& partitionData,
                                            const config::KVIndexConfigPtr& kvIndexConfig,
                                            const index_base::Version& version,
                                            std::vector<OnlineSegmentReader>& segmentReaders)
{
    assert(segmentReaders.empty());
    for (int64_t i = version.GetSegmentCount() - 1; i >= 0; i--) {
        OnlineSegmentReader segmentReader;
        segmentReader.Open(kvIndexConfig, partitionData->GetSegmentData(version[i]));
        segmentReaders.push_back(segmentReader);
    }
}

bool KVReaderImpl::doCollectAllCode()
{
    INIT_CODEGEN_INFO(KVReaderImpl, true);
    COLLECT_CONST_MEM_VAR(mHasTTL);
    COLLECT_CONST_MEM_VAR(mHasBuildingSegment);
    COLLECT_CONST_MEM_VAR(mHasOnlineSegment);
    COLLECT_CONST_MEM_VAR(mHasMultiRegion);
    COLLECT_CONST_MEM_VAR(mHasSearchCache);
    COLLECT_CONST_MEM_VAR(mKVReportMetrics);
    for (size_t i = 0; i < mOfflineSegmentReaders.size(); i++) {
        if (mOfflineSegmentReaders[i].size() > 0) {
            mOfflineSegmentReaders[i][0].collectAllCode();
            COLLECT_TYPE_REDEFINE(OfflineSegmentReader, mOfflineSegmentReaders[i][0].getTargetClassName());
            combineCodegenInfos(&mOfflineSegmentReaders[i][0]);
            break;
        }
    }
    if (mOnlineSegmentReaders.size() > 0) {
        mOnlineSegmentReaders[0].collectAllCode();
        COLLECT_TYPE_REDEFINE(OnlineSegmentReader, mOnlineSegmentReaders[0].getTargetClassName());
        combineCodegenInfos(&mOnlineSegmentReaders[0]);
    }
    return true;
}

void KVReaderImpl::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    CodegenCheckerPtr checker(new CodegenChecker);
    COLLECT_CONST_MEM(checker, mHasTTL);
    COLLECT_CONST_MEM(checker, mHasBuildingSegment);
    COLLECT_CONST_MEM(checker, mHasOnlineSegment);
    COLLECT_CONST_MEM(checker, mHasMultiRegion);
    COLLECT_CONST_MEM(checker, mHasSearchCache);
    COLLECT_CONST_MEM(checker, mKVReportMetrics);
    COLLECT_TYPE_DEFINE(checker, OfflineSegmentReader);
    COLLECT_TYPE_DEFINE(checker, OnlineSegmentReader);
    for (size_t i = 0; i < mOfflineSegmentReaders.size(); i++) {
        if (mOfflineSegmentReaders[i].size() > 0) {
            mOfflineSegmentReaders[i][0].TEST_collectCodegenResult(checkers, "Offline");
            break;
        }
    }
    if (mOnlineSegmentReaders.size() > 0) {
        mOnlineSegmentReaders[0].TEST_collectCodegenResult(checkers, "Online");
    }

    checkers["KVReaderImpl"] = checker;
}

void KVReaderImpl::LoadSegmentReaders(const index_base::PartitionDataPtr& partitionData,
                                      const config::KVIndexConfigPtr& kvIndexConfig, const index_base::Version& version,
                                      std::vector<std::vector<OfflineSegmentReader>>& segmentReaders)
{
    assert(segmentReaders.empty());
    if (version.GetSegmentCount() == 0) {
        return;
    }
    const indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
    if (levelInfo.GetTopology() == indexlibv2::framework::topo_sequence && levelInfo.GetLevelCount() > 1) {
        IE_LOG(ERROR, "kv reader does not support sequence topology with more than one level");
        assert(false);
        return;
    }
    size_t columnCount = levelInfo.GetShardCount();
    assert(columnCount > 0);
    segmentReaders.resize(columnCount);
    for (size_t columnIdx = 0; columnIdx < columnCount; ++columnIdx) {
        std::vector<segmentid_t> segIds = levelInfo.GetSegmentIds(columnIdx);
        for (size_t segIdx = 0; segIdx < segIds.size(); ++segIdx) {
            OfflineSegmentReader segmentReader;
            segmentReader.Open(kvIndexConfig, partitionData->GetSegmentData(segIds[segIdx]));
            segmentReaders[columnIdx].push_back(segmentReader);
        }
    }
}
}} // namespace indexlib::index
