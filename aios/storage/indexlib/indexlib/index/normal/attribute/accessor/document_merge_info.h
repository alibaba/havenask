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

namespace indexlib { namespace index {

struct DocumentMergeInfo {
    DocumentMergeInfo() : segmentIndex(-1), oldDocId(INVALID_DOCID), newDocId(INVALID_DOCID), targetSegmentIndex(0) {}

    DocumentMergeInfo(int32_t segIdx, docid_t oldId, docid_t newId, uint32_t targetSegIdx)
        : segmentIndex(segIdx)
        , oldDocId(oldId)
        , newDocId(newId)
        , targetSegmentIndex(targetSegIdx)
    {
    }

    int32_t segmentIndex;
    docid_t oldDocId;
    docid_t newDocId; // docid in plan, not in target segment local; TODO: use target segment localId
    uint32_t targetSegmentIndex;
};

DEFINE_SHARED_PTR(DocumentMergeInfo);
}} // namespace indexlib::index
