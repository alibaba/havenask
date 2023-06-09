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
#include "indexlib/table/common/CommonTabletLoader.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, CommonTabletLoader);

Status CommonTabletLoader::DoPreLoad(const framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                                     const framework::Version& newOnDiskVersion)
{
    if (newOnDiskVersionSegments.size() != newOnDiskVersion.GetSegmentCount()) {
        AUTIL_LOG(ERROR, "new segments count [%lu] is not equal to new version's segment count [%lu]",
                  newOnDiskVersionSegments.size(), newOnDiskVersion.GetSegmentCount());
        return Status::InvalidArgs();
    }
    size_t segmentIdx = 0;
    for (auto [segId, _] : newOnDiskVersion) {
        if (segId != newOnDiskVersionSegments[segmentIdx]->GetSegmentId()) {
            AUTIL_LOG(ERROR, "version segment id[%d] and [%d] inconsistent", segId,
                      newOnDiskVersionSegments[segmentIdx]->GetSegmentId());
            return Status::InvalidArgs();
        }
        ++segmentIdx;
    }
    _segmentIdsOnPreload.clear();
    _newVersion = newOnDiskVersion.Clone();
    auto locator = _newVersion.GetLocator();
    if (lastTabletData.GetLocator().IsValid() && !locator.IsSameSrc(lastTabletData.GetLocator(), true)) {
        AUTIL_LOG(INFO, "tablet data locator [%s] src not equal new version locator [%s] src, will drop rt",
                  lastTabletData.GetLocator().DebugString().c_str(), locator.DebugString().c_str());
        _newSegments = std::move(newOnDiskVersionSegments);
        _dropRt = true;
        return Status::OK();
    }

    _tabletName = lastTabletData.GetTabletName();
    if (_fenceName == newOnDiskVersion.GetFenceName()) {
        std::vector<std::shared_ptr<framework::Segment>> allSegments;
        allSegments.insert(allSegments.end(), newOnDiskVersionSegments.begin(), newOnDiskVersionSegments.end());
        auto slice = lastTabletData.CreateSlice();
        for (auto oldSegment : slice) {
            _segmentIdsOnPreload.insert(oldSegment->GetSegmentId());
            if (_newVersion.HasSegment(oldSegment->GetSegmentId())) {
                continue;
            }
            framework::Locator segLocator = oldSegment->GetSegmentInfo()->GetLocator();
            if (!segLocator.IsSameSrc(locator, true)) {
                AUTIL_LOG(INFO, "segment [%d] locator [%s] src not equal with version locator [%s] src",
                          oldSegment->GetSegmentId(), segLocator.DebugString().c_str(), locator.DebugString().c_str());
                continue;
            }
            auto compareResult = locator.IsFasterThan(segLocator, true);
            if (compareResult != framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
                AUTIL_LOG(WARN, "add segment [%d]", oldSegment->GetSegmentId());
                allSegments.push_back(oldSegment);
            }
        }
        _newSegments = std::move(allSegments);
        return Status::OK();
    } else {
        // drop realtime
        _newSegments = std::move(newOnDiskVersionSegments);
        _dropRt = true;
        return Status::OK();
    }
}

std::pair<Status, std::unique_ptr<framework::TabletData>>
CommonTabletLoader::FinalLoad(const framework::TabletData& currentTabletData)
{
    if (!_dropRt) {
        auto slice = currentTabletData.CreateSlice();
        for (auto segment : slice) {
            if (_segmentIdsOnPreload.find(segment->GetSegmentId()) == _segmentIdsOnPreload.end()) {
                _newSegments.emplace_back(segment);
                AUTIL_LOG(WARN, "add segment [%d]", segment->GetSegmentId());
            }
        }
    }
    auto newTabletData = std::make_unique<framework::TabletData>(_tabletName);
    Status status =
        newTabletData->Init(_newVersion.Clone(), std::move(_newSegments), currentTabletData.GetResourceMap()->Clone());
    return {status, std::move(newTabletData)};
}
} // namespace indexlibv2::table
