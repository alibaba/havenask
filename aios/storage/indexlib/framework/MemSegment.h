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

#include <vector>

#include "indexlib/base/Status.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/Segment.h"

namespace indexlib::util {
class BuildResourceMetrics;
}

namespace indexlib::framework {
class SegmentMetrics;
}
namespace indexlibv2::config {
class TabletSchema;
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace indexlibv2::framework {

class SegmentDumpItem;

class MemSegment : public Segment
{
public:
    MemSegment(const SegmentMeta& segmentMeta) : Segment(SegmentStatus::ST_BUILDING) { SetSegmentMeta(segmentMeta); }
    virtual ~MemSegment() = default;

public:
    size_t EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema) override { return 0; }

public:
    virtual Status Open(const BuildResource& resource, indexlib::framework::SegmentMetrics* lastSegmentMetrics) = 0;
    virtual Status Build(document::IDocumentBatch* batch) = 0;
    virtual std::shared_ptr<indexlib::util::BuildResourceMetrics> GetBuildResourceMetrics() const = 0;
    virtual bool NeedDump() const = 0;
    virtual std::pair<Status, std::vector<std::shared_ptr<SegmentDumpItem>>> CreateSegmentDumpItems() = 0;
    virtual void EndDump() = 0;
    virtual void Seal() = 0;
    virtual bool IsDirty() const = 0;
};

} // namespace indexlibv2::framework
