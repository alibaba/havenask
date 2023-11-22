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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

struct OutputSegmentMergeInfo {
public:
    OutputSegmentMergeInfo() = default;
    ~OutputSegmentMergeInfo() {}

    segmentid_t targetSegmentId = INVALID_SEGMENTID;
    segmentindex_t targetSegmentIndex = 0;
    std::string path;
    std::string temperatureLayer;
    file_system::DirectoryPtr directory;
    size_t docCount = 0;
};

DEFINE_SHARED_PTR(OutputSegmentMergeInfo);

typedef std::vector<OutputSegmentMergeInfo> OutputSegmentMergeInfos;
}} // namespace indexlib::index_base
