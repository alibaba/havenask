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
#include "indexlib/index/inverted_index/InvertedIndexerOrganizerUtil.h"

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/common/IndexerOrganizerUtil.h"
#include "indexlib/index/inverted_index/IInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/IInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedMemIndexer.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexerOrganizerUtil);

Status InvertedIndexerOrganizerUtil::UpdateOneIndexForReplay(docid_t docId,
                                                             const IndexerOrganizerMeta& indexerOrganizerMeta,
                                                             SingleInvertedIndexBuildInfoHolder* buildInfoHolder,
                                                             const document::ModifiedTokens& modifiedTokens)
{
    if (docId < indexerOrganizerMeta.realtimeBaseDocId) {
        assert(docId < indexerOrganizerMeta.dumpingBaseDocId);
        auto status = UpdateOneSegmentDiskIndex<MultiShardInvertedDiskIndexer>(
            buildInfoHolder, docId, indexerOrganizerMeta, modifiedTokens, buildInfoHolder->shardId);
        UpdateBuildResourceMetrics(buildInfoHolder);
        return status;
    }
    return Status::OK();
}

Status InvertedIndexerOrganizerUtil::UpdateOneIndexForBuild(docid_t docId,
                                                            const IndexerOrganizerMeta& indexerOrganizerMeta,
                                                            SingleInvertedIndexBuildInfoHolder* buildInfoHolder,
                                                            const document::ModifiedTokens& modifiedTokens)
{
    if (docId < indexerOrganizerMeta.dumpingBaseDocId) {
        auto status = UpdateOneSegmentDiskIndex<MultiShardInvertedDiskIndexer>(
            buildInfoHolder, docId, indexerOrganizerMeta, modifiedTokens, buildInfoHolder->shardId);
        UpdateBuildResourceMetrics(buildInfoHolder);
        return status;
    }
    if (docId < indexerOrganizerMeta.buildingBaseDocId) {
        return UpdateOneSegmentDumpingIndex<MultiShardInvertedMemIndexer>(buildInfoHolder, docId, indexerOrganizerMeta,
                                                                          modifiedTokens, buildInfoHolder->shardId);
    }
    if (buildInfoHolder->buildingIndexer != nullptr) {
        if (buildInfoHolder->shardId != INVALID_SHARDID) {
            std::dynamic_pointer_cast<MultiShardInvertedMemIndexer>(buildInfoHolder->buildingIndexer)
                ->UpdateTokens(docId - indexerOrganizerMeta.buildingBaseDocId, modifiedTokens,
                               buildInfoHolder->shardId);
            return Status::OK();
        }
        buildInfoHolder->buildingIndexer->UpdateTokens(docId - indexerOrganizerMeta.buildingBaseDocId, modifiedTokens);
        return Status::OK();
    }
    return Status::InvalidArgs("DocId[%d] is invalid", docId);
}

template <typename castedMultiShardType>
Status
InvertedIndexerOrganizerUtil::UpdateOneSegmentDiskIndex(SingleInvertedIndexBuildInfoHolder* buildInfoHolder,
                                                        docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
                                                        const document::ModifiedTokens& modifiedTokens, size_t shardId)
{
    for (auto it = buildInfoHolder->diskIndexers.begin(); it != buildInfoHolder->diskIndexers.end(); ++it) {
        docid_t baseId = it->first.first;
        uint64_t segmentDocCount = it->first.second;
        if (docId < baseId or docId >= (baseId + segmentDocCount)) {
            continue;
        }
        if (unlikely(it->second == nullptr)) {
            std::shared_ptr<IInvertedDiskIndexer> diskIndexer = nullptr;
            auto key = std::make_pair(baseId, segmentDocCount);
            auto segmentIter = buildInfoHolder->diskSegments.find(key);
            if (segmentIter == buildInfoHolder->diskSegments.end()) {
                return Status::InternalError(
                    "Unable to find segment for docId[%d], dumpingBaseDocId [%d], buildingBaseDocId [%d]", docId,
                    indexerOrganizerMeta.dumpingBaseDocId, indexerOrganizerMeta.buildingBaseDocId);
            }
            indexlibv2::framework::Segment* segment = segmentIter->second;
            auto st = IndexerOrganizerUtil::GetIndexer<IInvertedDiskIndexer, IInvertedMemIndexer>(
                segment, buildInfoHolder->outerIndexConfig, &diskIndexer, /*dumpingMemIndexer=*/nullptr,
                /*buildingMemIndexer=*/nullptr);
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "Get disk indexer failed, st[%s]", st.ToString().c_str());
                return st;
            }
            assert(diskIndexer != nullptr);
            it->second = diskIndexer;
        }
        if (shardId != INVALID_SHARDID) {
            std::dynamic_pointer_cast<castedMultiShardType>(it->second)
                ->UpdateTokens(docId - baseId, modifiedTokens, shardId);
            return Status::OK();
        }
        it->second->UpdateTokens(docId - baseId, modifiedTokens);
        return Status::OK();
    }
    AUTIL_INTERVAL_LOG2(
        60, WARN,
        "Unable to find disk segment in updating inverted index, this is OK in offline build, but fatal in "
        "online build");
    return Status::OK();
}

template <typename castedMultiShardType>
Status InvertedIndexerOrganizerUtil::UpdateOneSegmentDumpingIndex(SingleInvertedIndexBuildInfoHolder* buildInfoHolder,
                                                                  docid_t docId,
                                                                  const IndexerOrganizerMeta& indexerOrganizerMeta,
                                                                  const document::ModifiedTokens& modifiedTokens,
                                                                  size_t shardId)
{
    for (auto it = buildInfoHolder->dumpingIndexers.begin(); it != buildInfoHolder->dumpingIndexers.end(); ++it) {
        docid_t baseId = it->first.first;
        uint64_t segmentDocCount = it->first.second;
        if (docId < baseId or docId >= (baseId + segmentDocCount)) {
            continue;
        }
        if (unlikely(it->second == nullptr)) {
            return Status::InternalError(
                "Unable to find dumping indexer for docId[%d], dumpingBaseDocId [%d], buildingBaseDocId [%d]", docId,
                indexerOrganizerMeta.dumpingBaseDocId, indexerOrganizerMeta.buildingBaseDocId);
        }
        if (shardId != INVALID_SHARDID) {
            std::dynamic_pointer_cast<castedMultiShardType>(it->second)
                ->UpdateTokens(docId - baseId, modifiedTokens, shardId);
            return Status::OK();
        }
        it->second->UpdateTokens(docId - baseId, modifiedTokens);
        return Status::OK();
    }
    return Status::InternalError("Unable to find segment for docId[%d], dumpingBaseDocId [%d], buildingBaseDocId [%d]",
                                 docId, indexerOrganizerMeta.dumpingBaseDocId, indexerOrganizerMeta.buildingBaseDocId);
}

void InvertedIndexerOrganizerUtil::UpdateBuildResourceMetrics(SingleInvertedIndexBuildInfoHolder* buildInfoHolder)
{
    InvertedIndexMetrics::SingleFieldIndexMetrics* metrics = buildInfoHolder->singleFieldIndexMetrics;
    if (metrics == nullptr) {
        return;
    }

    size_t poolMemory = 0;
    size_t totalMemory = 0;
    size_t totalRetiredMemory = 0;
    size_t totalDocCount = 0;
    size_t totalAlloc = 0;
    size_t totalFree = 0;
    size_t totalTreeCount = 0;
    for (const auto& segmentIndexer : buildInfoHolder->diskIndexers) {
        if (segmentIndexer.second != nullptr) {
            segmentIndexer.second->UpdateBuildResourceMetrics(poolMemory, totalMemory, totalRetiredMemory,
                                                              totalDocCount, totalAlloc, totalFree, totalTreeCount);
        }
    }

    metrics->dynamicIndexDocCount = totalDocCount;
    metrics->dynamicIndexMemory = totalMemory;
    metrics->dynamicIndexRetiredMemory = totalRetiredMemory;
    metrics->dynamicIndexTotalAllocatedMemory = totalAlloc;
    metrics->dynamicIndexTotalFreedMemory = totalFree;
    metrics->dynamicIndexTreeCount = totalTreeCount;
}

bool InvertedIndexerOrganizerUtil::ShouldSkipUpdateIndex(const indexlibv2::config::InvertedIndexConfig* indexConfig)
{
    if (indexConfig == nullptr) {
        return true;
    }
    assert(indexConfig->GetShardingType() !=
           indexlibv2::config::InvertedIndexConfig::IndexShardingType::IST_IS_SHARDING);
    if (indexConfig->IsDisabled() || indexConfig->IsDeleted()) {
        return true;
    }
    if (!indexConfig->IsIndexUpdatable()) {
        return true;
    }
    if (!indexConfig->GetNonTruncateIndexName().empty()) {
        return true;
    }
    return false;
}

const std::vector<document::ModifiedTokens>&
InvertedIndexerOrganizerUtil::GetModifiedTokens(indexlibv2::document::IDocument* doc)
{
    static const std::vector<document::ModifiedTokens> EMPTY_MODIFIED_TOKENS;
    if (doc == nullptr) {
        return EMPTY_MODIFIED_TOKENS;
    }
    auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (normalDoc == nullptr) {
        return EMPTY_MODIFIED_TOKENS;
    }
    if (UPDATE_FIELD != normalDoc->GetDocOperateType()) {
        return EMPTY_MODIFIED_TOKENS;
    }
    auto indexDoc = normalDoc->GetIndexDocument();
    if (indexDoc == nullptr) {
        return EMPTY_MODIFIED_TOKENS;
    }
    auto docId = indexDoc->GetDocId();
    if (INVALID_DOCID == docId) {
        return EMPTY_MODIFIED_TOKENS;
    }
    return indexDoc->GetModifiedTokens();
}

} // namespace indexlib::index
