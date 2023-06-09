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
#pragma once

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/framework/SegmentDumpItem.h"
#include "indexlib/framework/SegmentMetrics.h"

namespace indexlibv2::index {
class IMemIndexer;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::plain {

class SegmentMetricsDumpItem : public framework::SegmentDumpItem
{
public:
    SegmentMetricsDumpItem(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics,
                           const std::shared_ptr<indexlib::file_system::Directory>& dir);

    SegmentMetricsDumpItem(const SegmentMetricsDumpItem&) = delete;
    SegmentMetricsDumpItem& operator=(const SegmentMetricsDumpItem&) = delete;
    SegmentMetricsDumpItem(SegmentMetricsDumpItem&&) = delete;
    SegmentMetricsDumpItem& operator=(SegmentMetricsDumpItem&&) = delete;

    ~SegmentMetricsDumpItem() = default;

    Status Dump() noexcept override;
    bool IsDumped() const override { return _dumped; }

private:
    std::shared_ptr<indexlib::framework::SegmentMetrics> _segmentMetrics;
    std::shared_ptr<indexlib::file_system::Directory> _dir;
    bool _dumped;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
