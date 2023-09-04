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
#include "indexlib/table/plain/MultiShardDiskSegment.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/table/plain/MultiShardSegmentMetrics.h"

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.plain, MultiShardDiskSegment);

Status MultiShardDiskSegment::Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode)
{
    auto segmentInfo = GetSegmentInfo();
    uint32_t shardCount = segmentInfo->GetShardCount();
    _shardSegments.resize(shardCount);
    uint32_t shardingShardId = segmentInfo->shardId;
    if (shardCount == 1 || shardingShardId != framework::SegmentInfo::INVALID_SHARDING_ID) {
        if (shardCount == 1) {
            // for legacy code
            shardingShardId = 0;
        }
        auto status = OpenSingleShardSegment(shardingShardId, memoryQuotaController, mode);
        RETURN_IF_STATUS_ERROR(status, "open segment failed");
    } else {
        // load build multi shard segment
        assert(shardingShardId == framework::SegmentInfo::INVALID_SHARDING_ID);
        auto status = OpenBuildSegment(memoryQuotaController, mode);
        RETURN_IF_STATUS_ERROR(status, "open shard segment failed");
    }
    _segmentMeta.segmentMetrics.reset(new MultiShardSegmentMetrics());
    auto multiShardMetrics = (MultiShardSegmentMetrics*)_segmentMeta.segmentMetrics.get();
    for (size_t i = 0; i < _shardSegments.size(); i++) {
        if (_shardSegments[i]) {
            multiShardMetrics->AddSegmentMetrics(i, _shardSegments[i]->GetSegmentMetrics());
        }
    }
    return Status::OK();
}

Status MultiShardDiskSegment::OpenBuildSegment(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                                               OpenMode mode)
{
    uint32_t shardCount = GetSegmentInfo()->GetShardCount();
    for (uint32_t i = 0; i < shardCount; i++) {
        framework::SegmentMeta segMeta;
        segMeta.segmentId = GetSegmentId();
        segMeta.segmentInfo.reset(new framework::SegmentInfo());
        *(segMeta.segmentInfo) = *GetSegmentInfo();
        auto shardDirName = PathUtil::GetShardDirName(i);
        auto result = GetSegmentDirectory()->GetIDirectory()->GetDirectory(shardDirName);
        RETURN_IF_STATUS_ERROR(result.Status(), "get shard dir [%s] failed, segment dir [%s]", shardDirName.c_str(),
                               GetSegmentDirectory()->DebugString().c_str());
        segMeta.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(result.result);
        segMeta.schema = _tabletSchema;
        _shardSegments[i].reset(new PlainDiskSegment(_tabletSchema, segMeta, _buildResource));
        auto status = _shardSegments[i]->Open(memoryQuotaController, mode);
        RETURN_IF_STATUS_ERROR(status, "open sharding shard [%d] segment [%d] failed", i, GetSegmentId());
    }
    return Status::OK();
}

Status MultiShardDiskSegment::OpenSingleShardSegment(
    uint32_t shardId, const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode)
{
    _shardSegments[shardId].reset(new PlainDiskSegment(_tabletSchema, _segmentMeta, _buildResource));
    return _shardSegments[shardId]->Open(memoryQuotaController, mode);
}

void MultiShardDiskSegment::CollectSegmentDescription(const std::shared_ptr<framework::SegmentDescriptions>& segDescs)
{
    auto levelInfo = segDescs->GetLevelInfo();
    assert(levelInfo != nullptr);
    std::string segDirName = PathUtil::GetFileNameFromPath(GetSegmentDirectory()->GetPhysicalPath(""));
    uint32_t levelIdx = PathUtil::GetLevelIdxFromSegmentDirName(segDirName);
    auto& levelMeta = levelInfo->levelMetas[levelIdx];
    if (levelIdx == 0) {
        levelMeta.segments.push_back(GetSegmentId());
    } else {
        uint32_t shardIdx = GetSegmentInfo()->GetShardId();
        assert(shardIdx != framework::SegmentInfo::INVALID_SHARDING_ID);
        assert(shardIdx < levelMeta.segments.size());
        levelMeta.segments[shardIdx] = GetSegmentId();
    }
}

std::shared_ptr<framework::Segment> MultiShardDiskSegment::GetShardSegment(size_t shardIdx) const
{
    if (shardIdx >= _shardSegments.size()) {
        return nullptr;
    }
    return _shardSegments[shardIdx];
}

size_t MultiShardDiskSegment::EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto segmentInfo = GetSegmentInfo();
    auto shardCount = segmentInfo->GetShardCount();
    uint32_t shardingShardId = segmentInfo->shardId;
    if (shardCount == 1 || shardingShardId != framework::SegmentInfo::INVALID_SHARDING_ID) {
        auto diskSegment = std::make_unique<PlainDiskSegment>(_tabletSchema, _segmentMeta, _buildResource);
        return diskSegment->EstimateMemUsed(schema);
    } else {
        size_t memEstimate = 0;
        assert(segmentInfo->shardId == framework::SegmentInfo::INVALID_SHARDING_ID);
        for (size_t i = 0; i < shardCount; i++) {
            framework::SegmentMeta segMeta;
            segMeta.segmentId = GetSegmentId();
            segMeta.segmentInfo.reset(new framework::SegmentInfo());
            *(segMeta.segmentInfo) = *segmentInfo;
            auto shardDirName = PathUtil::GetShardDirName(i);
            auto result = GetSegmentDirectory()->GetIDirectory()->GetDirectory(shardDirName);
            if (!result.Status().IsOK()) {
                AUTIL_LOG(ERROR, "get shard dir [%s] failed, segment dir [%s] [%s]", shardDirName.c_str(),
                          GetSegmentDirectory()->DebugString().c_str(), result.Status().ToString().c_str());
                continue;
            }
            segMeta.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(result.result);
            segMeta.schema = _tabletSchema;
            auto diskSegment = std::make_unique<PlainDiskSegment>(_tabletSchema, segMeta, _buildResource);
            memEstimate += diskSegment->EstimateMemUsed(schema);
        }
        return memEstimate;
    }
}

size_t MultiShardDiskSegment::EvaluateCurrentMemUsed()
{
    size_t totalMemUsed = 0;
    for (size_t i = 0; i < _shardSegments.size(); i++) {
        if (_shardSegments[i]) {
            totalMemUsed += _shardSegments[i]->EvaluateCurrentMemUsed();
        }
    }
    return totalMemUsed;
}

void MultiShardDiskSegment::TEST_AddSegment(const std::shared_ptr<PlainDiskSegment>& segment)
{
    _shardSegments.emplace_back(segment);
}

} // namespace indexlibv2::plain
