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
#ifndef __INDEXLIB_SEGMENT_BASE_DOC_ID_INFO_H
#define __INDEXLIB_SEGMENT_BASE_DOC_ID_INFO_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

struct SegmentBaseDocIdInfo {
    SegmentBaseDocIdInfo() : segId(INVALID_SEGMENTID), baseDocId(INVALID_DOCID) {}

    SegmentBaseDocIdInfo(segmentid_t segmentId, docid_t docId) : segId(segmentId), baseDocId(docId) {}

    segmentid_t segId;
    docid_t baseDocId;
};
}} // namespace indexlib::index

#endif //__INDEXLIB_SEGMENT_MODIFIER_CONTAINER_H
