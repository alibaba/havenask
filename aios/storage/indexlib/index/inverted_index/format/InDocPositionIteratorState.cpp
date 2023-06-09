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
#include "indexlib/index/inverted_index/format/InDocPositionIteratorState.h"

#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InDocPositionIteratorState);

tf_t InDocPositionIteratorState::GetTermFreq()
{
    if (_termFreq == 0 && _option.HasTfBitmap()) {
        PositionBitmapReader* tfBitmap = _posSegDecoder->GetPositionBitmapReader();
        PosCountInfo info = tfBitmap->GetPosCountInfo(GetSeekedDocCount());
        _termFreq = info.currentDocPosCount;
    }
    return _termFreq;
}

} // namespace indexlib::index
