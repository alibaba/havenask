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
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

Status CommonTabletLoader::DoPreLoad(const framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                                     const framework::Version& newOnDiskVersion)
{
    _segmentIdsOnPreload.clear();
    _newVersion = newOnDiskVersion.Clone();
    auto locator = _newVersion.GetLocator();
    if (lastTabletData.GetLocator().IsValid() && !locator.IsSameSrc(lastTabletData.GetLocator(), true)) {
        TABLET_LOG(INFO, "tablet data locator [%s] src not equal new version locator [%s] src, will drop rt",
                   lastTabletData.GetLocator().DebugString().c_str(), locator.DebugString().c_str());
        _newSegments = std::move(newOnDiskVersionSegments);
        _dropRt = true;
        return Status::OK();
    }

    auto compareResult = lastTabletData.GetLocator().IsFasterThan(newOnDiskVersion.GetLocator(), true);
    if (compareResult == framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER &&
        _fenceName == newOnDiskVersion.GetFenceName()) {
        auto slice = lastTabletData.CreateSlice();
        for (const auto& segment : slice) {
            _segmentIdsOnPreload.insert(segment->GetSegmentId());
        }
        auto [status, allSegments] = GetRemainSegments(lastTabletData, newOnDiskVersionSegments, newOnDiskVersion);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "get remain segments failed, tablet data locator [%s], new version locator [%s]",
                       lastTabletData.GetLocator().DebugString().c_str(), locator.DebugString().c_str());
            return status;
        }
        _newSegments = std::move(allSegments);
        return Status::OK();
    } else {
        // drop realtime
        TABLET_LOG(INFO, "drop rt, tablet data locator [%s] , new version locator [%s]",
                   lastTabletData.GetLocator().DebugString().c_str(), locator.DebugString().c_str());
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
                TABLET_LOG(WARN, "add segment [%d]", segment->GetSegmentId());
            }
        }
    }
    auto newTabletData = std::make_unique<framework::TabletData>(_tabletName);
    Status status =
        newTabletData->Init(_newVersion.Clone(), std::move(_newSegments), currentTabletData.GetResourceMap()->Clone());
    return {status, std::move(newTabletData)};
}
} // namespace indexlibv2::table
