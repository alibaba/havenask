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
#include "indexlib/table/plain/MultiShardMemSegment.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/table/plain/MultiShardSegmentMetrics.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.plain, MultiShardMemSegment);

Status MultiShardMemSegment::Open(const framework::BuildResource& resource,
                                  indexlib::framework::SegmentMetrics* lastSegmentMetrics)
{
    assert(resource.buildResourceMetrics != nullptr);
    _buildResourceMetrics = resource.buildResourceMetrics;
    _metricsNode = _buildResourceMetrics->AllocateNode();

    uint32_t shardCount = GetSegmentInfo()->GetShardCount();
    for (uint32_t shardId = 0u; shardId < shardCount; ++shardId) {
        auto directory = shardCount == 1 ? GetSegmentDirectory()
                                         : GetSegmentDirectory()->MakeDirectory(PathUtil::GetShardDirName(shardId));
        assert(directory != nullptr);
        RETURN_IF_STATUS_ERROR(OpenPlainMemSegment(resource, lastSegmentMetrics, directory, shardId), "open failed");
    }

    _segmentMeta.segmentMetrics = std::make_shared<MultiShardSegmentMetrics>();
    auto multiShardMetrics = (MultiShardSegmentMetrics*)_segmentMeta.segmentMetrics.get();
    for (uint32_t shardId = 0u; shardId < _shardSegments.size(); ++shardId) {
        multiShardMetrics->AddSegmentMetrics(shardId, _shardSegments[shardId]->GetSegmentMetrics());
    }
    UpdateMemUse();

    AUTIL_LOG(INFO, "open multi shard mem segment with dir[%s]", GetSegmentDirectory()->DebugString().c_str());
    return Status::OK();
}

Status MultiShardMemSegment::OpenPlainMemSegment(
    const framework::BuildResource& resource, indexlib::framework::SegmentMetrics* lastSegmentMetrics,
    const std::shared_ptr<indexlib::file_system::Directory>& singleShardSegmentDir, uint32_t shardId)
{
    framework::SegmentMeta segMeta;
    segMeta.segmentId = GetSegmentId();
    segMeta.segmentDir = singleShardSegmentDir;
    segMeta.segmentInfo = std::make_shared<framework::SegmentInfo>(*GetSegmentInfo());
    segMeta.segmentMetrics = std::make_shared<indexlib::framework::SegmentMetrics>();
    segMeta.schema = _tabletSchema;

    framework::BuildResource buildResource = resource;
    buildResource.buildingMemLimit = resource.buildingMemLimit / GetSegmentInfo()->GetShardCount();
    buildResource.buildResourceMetrics = nullptr;
    auto lastShardSegmentMetrics = GetShardSegmentMetrics(shardId, lastSegmentMetrics);

    auto shardSegment = _memSegmentCreator(_options, _tabletSchema, segMeta);
    RETURN_IF_STATUS_ERROR(shardSegment->Open(buildResource, lastShardSegmentMetrics), "open shard segment[%d] failed",
                           shardId);
    _shardSegments.push_back(shardSegment);
    return Status::OK();
}

size_t MultiShardMemSegment::EvaluateCurrentMemUsed()
{
    size_t totalMemUsed = 0;
    for (size_t i = 0; i < _shardSegments.size(); i++) {
        totalMemUsed += _shardSegments[i]->EvaluateCurrentMemUsed();
    }
    return totalMemUsed;
}

indexlib::framework::SegmentMetrics*
MultiShardMemSegment::GetShardSegmentMetrics(uint32_t shardId, indexlib::framework::SegmentMetrics* metrics)
{
    auto multiShardSegmentMetrics = dynamic_cast<MultiShardSegmentMetrics*>(metrics);
    if (multiShardSegmentMetrics == nullptr) {
        AUTIL_LOG(WARN, "dynamic cast to MultiShardSegmentMetrics failed");
        return nullptr;
    }
    return multiShardSegmentMetrics->GetSegmentMetrics(shardId).get();
}

void MultiShardMemSegment::CollectSegmentDescription(const std::shared_ptr<framework::SegmentDescriptions>& segDescs)
{
    assert(segDescs != nullptr);
    auto levelInfo = segDescs->GetLevelInfo();
    if (levelInfo == nullptr) {
        levelInfo.reset(new indexlibv2::framework::LevelInfo());
        levelInfo->Init(framework::topo_hash_mod, _levelNum, GetSegmentInfo()->GetShardCount());
        segDescs->SetLevelInfo(levelInfo);
    }
    auto& levelMeta = levelInfo->levelMetas[0];
    levelMeta.segments.push_back(GetSegmentId());
}

Status MultiShardMemSegment::Build(document::IDocumentBatch* batch)
{
    uint32_t shardCount = GetSegmentInfo()->GetShardCount();
    if (_shardPartitioner == nullptr) {
        assert(shardCount == 1);
        RETURN_IF_STATUS_ERROR(_shardSegments[0]->Build(batch), "build doc failed");
        UpdateSegmentInfo(batch);
        UpdateMemUse();
        return Status::OK();
    }
    assert(_shardPartitioner->GetShardCount() == shardCount);
    std::vector<std::shared_ptr<document::IDocumentBatch>> shardBatches(shardCount);
    for (size_t i = 0; i < batch->GetBatchSize(); i++) {
        if (batch->IsDropped(i)) {
            continue;
        }
        auto doc = (*batch)[i];
        uint64_t keyHash = ExtractKeyHash(doc);
        uint32_t shardIdx = 0;
        _shardPartitioner->GetShardIdx(keyHash, shardIdx);
        if (shardBatches[shardIdx] == nullptr) {
            shardBatches[shardIdx] = batch->Create();
        }
        shardBatches[shardIdx]->AddDocument(doc);
    }

    for (size_t i = 0; i < shardCount; i++) {
        if (shardBatches[i] == nullptr) {
            continue;
        }
        auto status = _shardSegments[i]->Build(shardBatches[i].get());
        UpdateMemUse();
        if (status.IsNeedDump()) {
            AUTIL_LOG(INFO, "segment [%d] need dump", GetSegmentId());
            return status;
        }
        RETURN_IF_STATUS_ERROR(status, "build doc failed");
    }
    UpdateSegmentInfo(batch);
    return Status::OK();
}

void MultiShardMemSegment::UpdateMemUse()
{
    int64_t currentMemUse = 0;
    int64_t dumpExpandMemSize = 0;
    int64_t dumpTmpMemUse = 0;
    int64_t dumpFileSize = 0;
    for (auto& segment : _shardSegments) {
        auto buildResourceMetrics = segment->GetBuildResourceMetrics();
        currentMemUse += buildResourceMetrics->GetValue(indexlib::util::BMT_CURRENT_MEMORY_USE);
        dumpExpandMemSize =
            std::max(dumpExpandMemSize, buildResourceMetrics->GetValue(indexlib::util::BMT_DUMP_EXPAND_MEMORY_SIZE));
        dumpTmpMemUse += buildResourceMetrics->GetValue(indexlib::util::BMT_DUMP_TEMP_MEMORY_SIZE);
        dumpFileSize += buildResourceMetrics->GetValue(indexlib::util::BMT_DUMP_FILE_SIZE);
    }
    _metricsNode->Update(indexlib::util::BMT_CURRENT_MEMORY_USE, currentMemUse);
    _metricsNode->Update(indexlib::util::BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandMemSize);
    _metricsNode->Update(indexlib::util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTmpMemUse);
    _metricsNode->Update(indexlib::util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

void MultiShardMemSegment::UpdateSegmentInfo(document::IDocumentBatch* batch)
{
    const auto& locator = batch->GetLastLocator();
    if (locator.IsValid()) {
        _segmentMeta.segmentInfo->SetLocator(locator);
    }
    if (_segmentMeta.segmentInfo->timestamp < batch->GetMaxTimestamp()) {
        _segmentMeta.segmentInfo->timestamp = batch->GetMaxTimestamp();
    }
    if (_segmentMeta.segmentInfo->maxTTL < batch->GetMaxTTL()) {
        _segmentMeta.segmentInfo->maxTTL = batch->GetMaxTTL();
    }

    _segmentMeta.segmentInfo->docCount += batch->GetAddedDocCount();
}

uint64_t MultiShardMemSegment::ExtractKeyHash(const std::shared_ptr<document::IDocument>& doc)
{
    // TODO: by yijie.zhang use doc extractor to replace kv doc
    auto kvDoc = dynamic_cast<document::KVDocument*>(doc.get());
    return kvDoc->GetPKeyHash();
}

bool MultiShardMemSegment::NeedDump() const
{
    if (_segmentMeta.segmentInfo->docCount >= _options->GetBuildConfig().GetMaxDocCount()) {
        AUTIL_LOG(INFO,
                  "DumpSegment for reach doc count limit : "
                  "docCount [%lu] over maxDocCount [%lu]",
                  _segmentMeta.segmentInfo->docCount, _options->GetBuildConfig().GetMaxDocCount());
        return true;
    }
    return false;
}

std::pair<Status, std::vector<std::shared_ptr<framework::SegmentDumpItem>>>
MultiShardMemSegment::CreateSegmentDumpItems()
{
    std::vector<std::shared_ptr<framework::SegmentDumpItem>> dumpItems;
    for (auto& shardSegment : _shardSegments) {
        auto [st, segDumpItems] = shardSegment->CreateSegmentDumpItems();
        RETURN2_IF_STATUS_ERROR(st, std::vector<std::shared_ptr<framework::SegmentDumpItem>> {},
                                "create dump item failed");
        dumpItems.insert(dumpItems.end(), segDumpItems.begin(), segDumpItems.end());
    }
    return {Status::OK(), {dumpItems}};
}

void MultiShardMemSegment::EndDump()
{
    for (auto& shardSegment : _shardSegments) {
        shardSegment->EndDump();
        UpdateMemUse();
    }
}

void MultiShardMemSegment::Seal()
{
    for (auto& shardSegment : _shardSegments) {
        shardSegment->Seal();
        UpdateMemUse();
    }
}

bool MultiShardMemSegment::IsDirty() const
{
    for (auto& shardSegment : _shardSegments) {
        if (shardSegment->IsDirty()) {
            return true;
        }
    }
    return false;
}

void MultiShardMemSegment::TEST_AddSegment(const std::shared_ptr<PlainMemSegment>& segment)
{
    _shardSegments.emplace_back(segment);
}

std::shared_ptr<PlainMemSegment>
MultiShardMemSegment::CreatePlainMemSegment(const config::TabletOptions* options,
                                            const std::shared_ptr<config::TabletSchema>& schema,
                                            const framework::SegmentMeta& segmentMeta)
{
    return std::make_shared<PlainMemSegment>(options, schema, segmentMeta);
}

} // namespace indexlibv2::plain
