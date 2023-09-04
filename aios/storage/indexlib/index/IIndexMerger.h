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

#include <any>
#include <map>
#include <memory>
#include <vector>

#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlib::file_system {
class RelocatableFolder;
}

namespace indexlibv2::framework {
struct SegmentMeta;
class Segment;
class IndexTaskResourceManager;
} // namespace indexlibv2::framework

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::index {

class IIndexMerger
{
public:
    struct SourceSegment {
        docid_t baseDocid = 0;
        std::shared_ptr<framework::Segment> segment;
    };
    struct SegmentMergeInfos {
        std::vector<SourceSegment> srcSegments;
        std::vector<std::shared_ptr<framework::SegmentMeta>> targetSegments;
        std::shared_ptr<indexlib::file_system::RelocatableFolder> relocatableGlobalRoot;
    };

public:
    IIndexMerger() = default;
    virtual ~IIndexMerger() = default;

    virtual Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                        const std::map<std::string, std::any>& params) = 0;
    virtual Status Merge(const SegmentMergeInfos& segMergeInfos,
                         const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) = 0;
};

} // namespace indexlibv2::index
