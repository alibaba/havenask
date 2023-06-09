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

#include "indexlib/base/Status.h"
#include "indexlib/framework/Segment.h"

namespace indexlibv2::config {
class TabletSchema;
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlibv2::framework {

class DiskSegment : public Segment
{
public:
    enum class OpenMode {
        NORMAL,
        LAZY, // load on demand (for offline).
    };

public:
    DiskSegment(const SegmentMeta& segmentMeta) : Segment(SegmentStatus::ST_BUILT) { SetSegmentMeta(segmentMeta); }

    virtual ~DiskSegment() = default;

    virtual Status Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode) = 0;
    // schemas: from current schema to target schema list
    virtual Status Reopen(const std::vector<std::shared_ptr<config::TabletSchema>>& schemas) = 0;
};

} // namespace indexlibv2::framework
