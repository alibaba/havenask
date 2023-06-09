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
#include "indexlib/framework/SegmentDumper.h"

#include <atomic>
#include <memory>
#include <mutex>

#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, SegmentDumper);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

Status SegmentDumper::StoreSegmentInfo()
{
    auto segmentDir = _dumpingSegment->GetSegmentDirectory();
    auto segmentInfo = _dumpingSegment->GetSegmentInfo();
    if (!segmentDir) {
        return Status::InternalError("segment dir is empty");
    }
    TABLET_LOG(INFO, "begin store segment_info for segment[%d], segment info [%s]", GetSegmentId(),
               autil::legacy::ToJsonString(segmentInfo).c_str());
    auto status = segmentInfo->Store(segmentDir->GetIDirectory());
    if (!status.IsOK()) {
        auto s =
            Status::IOError("store segment_info failed, segId:%d, error:%s", GetSegmentId(), status.ToString().c_str());
        TABLET_LOG(ERROR, "%s", s.ToString().c_str());
        return s;
    }
    return status;
}

Status SegmentDumper::Dump(future_lite::Executor* executor)
{
    indexlib::util::ScopeLatencyReporter scopeTime(GetdumpSegmentLatencyMetric().get());
    auto segId = GetSegmentId();
    auto [st, dumpItems] = _dumpingSegment->CreateSegmentDumpItems();
    RETURN_IF_STATUS_ERROR(st, "create dump param failed, segId[%d]", segId);

    for (size_t i = 0; i < dumpItems.size(); ++i) {
        auto& dumpItem = dumpItems[i];
        auto status = dumpItem->Dump();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "dump segment failed, segId[%d], error:%s", segId, status.ToString().c_str());
            return status;
        }
    }
    auto status = StoreSegmentInfo();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "dump segment failed, segId[%d]", segId);
        return status;
    }

    _dumpingSegment->EndDump();

    if (_dumpingSegment->GetSegmentDirectory()) {
        _dumpingSegment->GetSegmentDirectory()->FlushPackage();
    }
    TABLET_LOG(INFO, "dump segment[%d] success, time_used[%.3f]s", segId, scopeTime.GetTimer().done_sec());
    return Status::OK();
}

std::shared_ptr<MemSegment> SegmentDumper::TEST_GetDumpingSegment() const { return _dumpingSegment; }
} // namespace indexlibv2::framework
