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
#include "indexlib/table/common/LSMTabletLoader.h"

#include <any>

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, LSMTabletLoader);

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

LSMTabletLoader::LSMTabletLoader(const std::string& fenceName) : CommonTabletLoader(fenceName) {}

Status LSMTabletLoader::DoPreLoad(const framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                                  const framework::Version& newOnDiskVersion)
{
    _tabletName = lastTabletData.GetTabletName();
    _newSegments = std::move(newOnDiskVersionSegments);
    _newVersion = newOnDiskVersion.Clone();
    return Status::OK();
}

std::pair<Status, std::unique_ptr<framework::TabletData>>
LSMTabletLoader::FinalLoad(const framework::TabletData& currentTabletData)
{
    auto locator = _newVersion.GetLocator();
    bool dropRt = false;
    auto currentLocator = currentTabletData.GetLocator();
    if (!currentLocator.IsSameSrc(locator, true)) {
        TABLET_LOG(INFO, "new version locator[%s] src not equal with current tablet data locator[%s] src",
                   locator.DebugString().c_str(), currentLocator.DebugString().c_str());
        dropRt = true;
    } else {
        if (_isOnline) {
            auto compareResult = locator.IsFasterThan(currentLocator, true);
            if (compareResult != framework::Locator::LocatorCompareResult::LCR_SLOWER) {
                TABLET_LOG(WARN,
                           "new version[%d] locator[%s] is NOT slower than current locator[%s], realtime index will be "
                           "dropped",
                           _newVersion.GetVersionId(), locator.DebugString().c_str(),
                           currentLocator.DebugString().c_str());
                dropRt = true;
            }
        }
    }
    if (!dropRt) {
        auto segmentDesc = _newVersion.GetSegmentDescriptions();
        auto levelInfo = segmentDesc->GetLevelInfo();

        auto [status, allSegments] = GetRemainSegments(currentTabletData, _newSegments, _newVersion);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "get remain segments failed, tablet data locator [%s], new version locator [%s]",
                      currentTabletData.GetLocator().DebugString().c_str(),
                      _newVersion.GetLocator().DebugString().c_str());
            return {status, nullptr};
        }
        _newSegments = allSegments;

        for (const auto& segment : _newSegments) {
            if (_newVersion.HasSegment(segment->GetSegmentId())) {
                continue;
            }
            if (levelInfo && segment->GetSegmentStatus() == framework::Segment::SegmentStatus::ST_BUILT) {
                levelInfo->AddSegment(segment->GetSegmentId());
            }
        }
    }
    auto newTabletData = std::make_unique<framework::TabletData>(_tabletName);
    auto status =
        newTabletData->Init(_newVersion.Clone(), std::move(_newSegments), currentTabletData.GetResourceMap()->Clone());
    return {status, std::move(newTabletData)};
}

} // namespace indexlibv2::table
