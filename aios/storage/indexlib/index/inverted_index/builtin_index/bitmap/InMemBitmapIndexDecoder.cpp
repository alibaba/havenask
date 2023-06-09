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
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexDecoder.h"

#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InMemBitmapIndexDecoder);

InMemBitmapIndexDecoder::InMemBitmapIndexDecoder() : _bitmapItemCount(0), _bitmapData(nullptr) {}

InMemBitmapIndexDecoder::~InMemBitmapIndexDecoder() {}

void InMemBitmapIndexDecoder::Init(const BitmapPostingWriter* postingWriter)
{
    assert(postingWriter);
    _termMeta.SetPayload(postingWriter->GetTermPayload());
    _termMeta.SetDocFreq(postingWriter->GetDF());
    _termMeta.SetTotalTermFreq(postingWriter->GetTotalTF());

    MEMORY_BARRIER();

    const util::ExpandableBitmap* bitmap = postingWriter->GetBitmapData();
    _bitmapItemCount = postingWriter->GetLastDocId() + 1;
    MEMORY_BARRIER();
    _bitmapData = bitmap->GetData();
}
} // namespace indexlib::index
