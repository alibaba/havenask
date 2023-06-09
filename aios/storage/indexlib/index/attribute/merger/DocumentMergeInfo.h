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

namespace indexlibv2::index {

struct DocumentMergeInfo {
    DocumentMergeInfo() : segmentIndex(-1), oldDocId(INVALID_DOCID), newDocId(INVALID_DOCID), targetSegmentId(0) {}

    DocumentMergeInfo(int32_t segIdx, docid_t oldId, docid_t newId, uint32_t targetSegIdx)
        : segmentIndex(segIdx)
        , oldDocId(oldId)
        , newDocId(newId)
        , targetSegmentId(targetSegIdx)
    {
    }

    int32_t segmentIndex;
    docid_t oldDocId;
    docid_t newDocId;
    uint32_t targetSegmentId;
};

} // namespace indexlibv2::index
