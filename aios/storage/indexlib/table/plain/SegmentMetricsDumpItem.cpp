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
#include "indexlib/table/plain/SegmentMetricsDumpItem.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.plain, SegmentMetricsDumpItem);

SegmentMetricsDumpItem::SegmentMetricsDumpItem(
    const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics,
    const std::shared_ptr<indexlib::file_system::IDirectory>& dir)
    : _segmentMetrics(segmentMetrics)
    , _dir(dir)
    , _dumped(false)
{
}

Status SegmentMetricsDumpItem::Dump() noexcept
{
    AUTIL_LOG(INFO, "begin dump segment metrics [%s]", _dir->GetLogicalPath().c_str());
    auto status = _segmentMetrics->StoreSegmentMetrics(_dir);
    RETURN_IF_STATUS_ERROR(status, "store segment metrics failed");
    AUTIL_LOG(INFO, "end dump segment metrics");
    return status;
}

} // namespace indexlibv2::plain
