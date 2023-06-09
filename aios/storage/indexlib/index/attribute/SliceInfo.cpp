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
#include "indexlib/index/attribute/SliceInfo.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SliceInfo);

SliceInfo::SliceInfo(int64_t sliceCount, int64_t sliceIdx) : _sliceCount(sliceCount), _sliceIdx(sliceIdx) {}
SliceInfo::~SliceInfo() {}

void SliceInfo::GetDocRange(int64_t docCount, docid_t& beginDocId, docid_t& endDocId) const
{
    if (_sliceCount <= 1 || _sliceIdx == -1) {
        beginDocId = 0;
        endDocId = docCount - 1;
        return;
    }

    if (docCount < _sliceCount) {
        if (_sliceIdx == 0) {
            beginDocId = 0;
            endDocId = docCount - 1;
            return;
        }
        beginDocId = std::numeric_limits<docid_t>::max();
        endDocId = -1;
        return;
    }
    int64_t sliceDocCount = docCount / _sliceCount;
    beginDocId = sliceDocCount * _sliceIdx;
    if (_sliceIdx == _sliceCount - 1) {
        endDocId = docCount - 1;
    } else {
        endDocId = sliceDocCount + beginDocId - 1;
    }
}

int64_t SliceInfo::GetSliceDocCount(int64_t docCount) const
{
    docid_t beginDocId = INVALID_DOCID;
    docid_t endDocId = INVALID_DOCID;
    GetDocRange(docCount, beginDocId, endDocId);
    if (beginDocId > endDocId) {
        return 0;
    }
    return endDocId - beginDocId + 1;
}

} // namespace indexlibv2::index
