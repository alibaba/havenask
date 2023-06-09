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

#include <memory>

#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/SegmentInfo.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::framework {
class SegmentMetrics;
}

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class SegmentInfo;

struct SegmentMeta {
    SegmentMeta();
    SegmentMeta(segmentid_t segId);
    ~SegmentMeta() = default;

    segmentid_t segmentId;
    std::shared_ptr<indexlib::file_system::Directory> segmentDir;
    std::shared_ptr<SegmentInfo> segmentInfo;
    std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics;
    std::shared_ptr<config::TabletSchema> schema;
};

} // namespace indexlibv2::framework
