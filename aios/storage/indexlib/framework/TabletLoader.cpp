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

#include "autil/UnitUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletLoader);

void TabletLoader::Init(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                        const std::shared_ptr<config::TabletSchema>& schema,
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
    const auto& rawSegments = GetNeedOpenSegments(onDiskSegmentPairs);
    if (_isOnline) {
        auto estimateSegmentsMemsize = EstimateMemUsed(_schema, rawSegments);
        if (!_memReserver->Reserve(estimateSegmentsMemsize)) {
            auto status =
                Status::NoMem("[%s] no enough memory to load segments. estimateMemsize[%s], "
                              "freeQuota[%s], version [%d -> %d]",
                              lastTabletData.GetTabletName().c_str(),
                              autil::UnitUtil::GiBDebugString(estimateSegmentsMemsize).c_str(),
                              autil::UnitUtil::GiBDebugString(_memReserver->GetFreeQuota()).c_str(),
                              lastTabletData.GetOnDiskVersion().GetVersionId(), newOnDiskVersion.GetVersionId());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
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
            AUTIL_LOG(ERROR, "[%s] create segment[%d] failed, openMode[%d]", lastTabletData.GetTabletName().c_str(),
                      diskSegment->GetSegmentId(), (int)openMode);
            return st;
        }
    }
    return DoPreLoad(lastTabletData, std::move(onDiskSegments), newOnDiskVersion);
}

size_t TabletLoader::EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema,
                                     const std::vector<framework::Segment*>& segments)
{
    size_t totalMemUsed = 0;
    for (auto segment : segments) {
        totalMemUsed += segment->EstimateMemUsed(schema);
    }
    return totalMemUsed;
}

size_t TabletLoader::EvaluateCurrentMemUsed(const std::vector<framework::Segment*>& segments)
{
    size_t totalMemUsed = 0;
    for (auto segment : segments) {
        totalMemUsed += segment->EvaluateCurrentMemUsed();
    }
    return totalMemUsed;
}
} // namespace indexlibv2::framework
