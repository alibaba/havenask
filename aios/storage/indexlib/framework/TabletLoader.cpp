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
#include "indexlib/framework/TabletLoader.h"

#include <algorithm>
#include <assert.h>
#include <ext/alloc_traits.h>

#include "autil/UnitUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletLoader);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

void TabletLoader::Init(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                        const std::shared_ptr<config::ITabletSchema>& schema,
                        const std::shared_ptr<indexlib::util::MemoryReserver>& memReserver, bool isOnline)
{
    assert(memoryQuotaController);
    assert(schema);
    assert(memReserver);
    _memoryQuotaController = memoryQuotaController;
    _schema = schema;
    _memReserver = memReserver;
    _isOnline = isOnline;
}

std::vector<framework::Segment*> TabletLoader::GetNeedOpenSegments(const SegmentPairs& onDiskSegmentPairs) const
{
    std::vector<Segment*> rawSegments;
    std::for_each(onDiskSegmentPairs.cbegin(), onDiskSegmentPairs.cend(),
                  [&rawSegments](const std::pair<std::shared_ptr<Segment>, bool>& pair) {
                      bool needToOpen = pair.second;
                      if (needToOpen) {
                          rawSegments.emplace_back(pair.first.get());
                      }
                  });
    return rawSegments;
}

Status TabletLoader::PreLoad(const TabletData& lastTabletData, const SegmentPairs& onDiskSegmentPairs,
                             const Version& newOnDiskVersion)
{
    _tabletName = lastTabletData.GetTabletName();
    const auto& rawSegments = GetNeedOpenSegments(onDiskSegmentPairs);
    if (_isOnline) {
        auto [status, estimateSegmentsMemsize] = EstimateMemUsed(_schema, rawSegments);
        if (!status.IsOK()) {
            return status;
        }
        if (!_memReserver->Reserve(estimateSegmentsMemsize)) {
            auto status =
                Status::NoMem("[%s] no enough memory to load segments. estimateMemsize[%s], "
                              "freeQuota[%s], version [%d -> %d]",
                              lastTabletData.GetTabletName().c_str(),
                              autil::UnitUtil::GiBDebugString(estimateSegmentsMemsize).c_str(),
                              autil::UnitUtil::GiBDebugString(_memReserver->GetFreeQuota()).c_str(),
                              lastTabletData.GetOnDiskVersion().GetVersionId(), newOnDiskVersion.GetVersionId());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
    }

    std::vector<std::shared_ptr<framework::Segment>> onDiskSegments;
    for (const auto& [segment, needOpen] : onDiskSegmentPairs) {
        onDiskSegments.emplace_back(segment);
        if (!needOpen) {
            continue;
        }
        auto diskSegment = std::dynamic_pointer_cast<DiskSegment>(segment);
        assert(diskSegment != nullptr);
        auto openMode = _isOnline ? DiskSegment::OpenMode::NORMAL : DiskSegment::OpenMode::LAZY;
        auto st = diskSegment->Open(_memoryQuotaController, openMode);
        if (!st.IsOK()) {
            TABLET_LOG(ERROR, "[%s] create segment[%d] failed, openMode[%d]", lastTabletData.GetTabletName().c_str(),
                       diskSegment->GetSegmentId(), (int)openMode);
            return st;
        }
    }
    RETURN_IF_STATUS_ERROR(ValidatePreloadParams(onDiskSegments, newOnDiskVersion), "preload failed, params invalid");
    return DoPreLoad(lastTabletData, std::move(onDiskSegments), newOnDiskVersion);
}

std::pair<Status, size_t> TabletLoader::EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema,
                                                        const std::vector<framework::Segment*>& segments)
{
    size_t totalMemUsed = 0;
    for (auto segment : segments) {
        auto [status, segmentMemUse] = segment->EstimateMemUsed(schema);
        if (!status.IsOK()) {
            return {status, 0};
        }
        totalMemUsed += segmentMemUse;
    }
    return {Status::OK(), totalMemUsed};
}

size_t TabletLoader::EvaluateCurrentMemUsed(const std::vector<framework::Segment*>& segments)
{
    size_t totalMemUsed = 0;
    for (auto segment : segments) {
        totalMemUsed += segment->EvaluateCurrentMemUsed();
    }
    return totalMemUsed;
}

std::pair<Status, std::vector<std::shared_ptr<Segment>>>
TabletLoader::GetRemainSegments(const TabletData& tabletData, const std::vector<std::shared_ptr<Segment>>& diskSegmemts,
                                const Version& version) const
{
    auto versionLocator = version.GetLocator();
    std::vector<std::shared_ptr<Segment>> segments = diskSegmemts;
    auto slice = tabletData.CreateSlice();
    auto onDiskVersion = tabletData.GetOnDiskVersion();
    for (auto segment : slice) {
        if (Segment::IsMergedSegmentId(segment->GetSegmentId())) {
            continue;
        }
        if (version.HasSegment(segment->GetSegmentId())) {
            continue;
        }
        auto segLocator = segment->GetLocator();
        auto segmentStatus = segment->GetSegmentStatus();
        if (segmentStatus == Segment::SegmentStatus::ST_BUILDING &&
            (!segLocator.IsValid() || segLocator == versionLocator)) {
            TABLET_LOG(INFO, "remain segment [%d] from current tablet data", segment->GetSegmentId());
            segments.push_back(segment);
            continue;
        }
        if (segment->GetSegmentSchema()) {
            schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();
            if (segSchemaId != INVALID_SCHEMAID && version.GetReadSchemaId() != INVALID_SCHEMAID) {
                if (segSchemaId < version.GetReadSchemaId()) {
                    TABLET_LOG(INFO,
                               "reclaim status[%d] segment [%d], segment schema id[%u] is"
                               " smaller than new version read schema id[%u].",
                               (int)(segment->GetSegmentStatus()), segment->GetSegmentId(), segSchemaId,
                               version.GetReadSchemaId());
                    continue;
                }
            }
        }
        if (!versionLocator.IsSameSrc(segLocator, /*ignoreLegacyDiffSrc*/ true)) {
            TABLET_LOG(ERROR, "version src is diff from segemnt, version src [%ld], segment src [%ld], segment id [%d]",
                       versionLocator.GetSrc(), segLocator.GetSrc(), segment->GetSegmentId());
            return {Status::Corruption("version src is diff from segemnt"), {}};
        }
        auto compareResult = versionLocator.IsFasterThan(segLocator, true);
        if (compareResult != framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
            TABLET_LOG(INFO, "remain segment [%d] from current tablet data", segment->GetSegmentId());
            segments.push_back(segment);
        } else {
            TABLET_LOG(INFO, "reclaim segment [%d] from current tablet data", segment->GetSegmentId());
        }
    }
    return {Status::OK(), segments};
}

Status
TabletLoader::ValidatePreloadParams(const std::vector<std::shared_ptr<framework::Segment>>& newOnDiskVersionSegments,
                                    const framework::Version& newOnDiskVersion)
{
    if (newOnDiskVersionSegments.size() != newOnDiskVersion.GetSegmentCount()) {
        return Status::InvalidArgs("new segments count is not equal to new version's segment count ",
                                   newOnDiskVersionSegments.size(), " vs ", newOnDiskVersion.GetSegmentCount());
    }
    size_t segmentIdx = 0;
    for (auto [segId, _] : newOnDiskVersion) {
        if (segId != newOnDiskVersionSegments[segmentIdx]->GetSegmentId()) {
            return Status::InvalidArgs("version segment id ", segId, " and ",
                                       newOnDiskVersionSegments[segmentIdx]->GetSegmentId(), " inconsistent");
        }
        ++segmentIdx;
    }
    return Status::OK();
}

} // namespace indexlibv2::framework
